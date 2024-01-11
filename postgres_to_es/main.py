import logging
import os
import time
import uuid
from datetime import datetime

from dotenv import load_dotenv

from services.es_loader import ESLoader
from services.pg_extractor import PostgresExtractor
from state.state import state
from utils.schemas import PGObject

load_dotenv(dotenv_path='../etl/.env')

logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)

BLOCK_SIZE = 1000

PAUSE_DURATION = 60

DSN = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('POSTGRES_HOST', 'localhost'),
    'port': os.environ.get('POSTGRES_PORT', 5432),
    'options': '-c search_path=content'
}

ES_PARAMS = [{
    'scheme': os.environ.get('ES_SCHEME'),
    'host': os.environ.get('ES_HOST'),
    'port': int(os.environ.get('ES_PORT')),
}]


class ETL:

    def __init__(self, pg_extractor: PostgresExtractor, es_loader: ESLoader):
        """Initialize ETL class."""

        self.pg_extractor = pg_extractor
        self.es_loader = es_loader

    def schemas_to_ids(self, objects: list) -> list:
        """Extract id of each object in list and return list of ids."""

        id_list = [str(obj.id) for obj in objects]
        return id_list

    def check_state(self) -> None:
        """Check if necessary state values exist or create base values."""

        last_modified = [
            'last_modified_person',
            'last_modified_genre',
            'last_modified_filmwork',
        ]

        last_processed_ids = [
            'last_filmwork_id',
            'last_genre_id',
            'last_person_id',
        ]

        for modified in last_modified:
            if not state.get_state(modified):
                state.set_state(modified, str(datetime(2000, 1, 1)))

        for id in last_processed_ids:
            if not state.get_state(id):
                state.set_state(id, str(uuid.uuid4()))

    def run(self) -> None:
        """Main ETL proccess function."""

        logging.info('ETL proccess started.')

        self.check_state()
        self.es_loader.create_index()
        pg_connection = self.pg_extractor._open_connection()

        # last modified states
        last_modified_person = state.get_state('last_modified_person')
        last_modified_genre = state.get_state('last_modified_genre')
        last_modified_filmwork = state.get_state('last_modified_filmwork')

        # last proccessed id states
        last_person_id = state.get_state('last_person_id')
        last_genre_id = state.get_state('last_genre_id')
        last_filmwork_id = state.get_state('last_filmwork_id')

        # Run etl proccess with modified persons
        last_person = self.filmworks_by_modified_persons(
            last_modified_person,
            last_person_id,
            pg_connection,
        )
        
        # Run etl proccess with modified genres
        last_genre = self.filmworks_by_modified_genres(
            last_modified_genre,
            last_genre_id,
            pg_connection,
        )

        # Run etl proccess with modified filmworks
        last_filmwork = self.modified_filmworks(
            last_modified_filmwork,
            last_filmwork_id,
            pg_connection,
        )

        # # Rewrite last modified states
        last_ids = {
            'last_person_id': last_person.id,
            'last_genre_id': last_genre.id,
            'last_filmwork_id': last_filmwork.id,
        }
        for key, value in last_ids.items():
            state.set_state(key, str(value))

        # Rewrite last proccessed id states
        last_modified = {
            'last_modified_person': last_person.modified_at,
            'last_modified_genre': last_genre.modified_at,
            'last_modified_filmwork': last_filmwork.modified_at,
        }
        for key, value in last_modified.items():
            state.set_state(key, str(value))

        self.pg_extractor._close_connection()

        logging.info('ETL proccess stopped.')
    
    def filmworks_by_modified_persons(
        self, last_modified: datetime, last_id: str, connection
    ) -> PGObject:
        persons = self.pg_extractor.extract_modified_persons(
            last_modified,
            last_id,
            connection,
        )

        if persons:
            last_person = persons[-1]
            person_ids = self.schemas_to_ids(persons)
            filmworks = (
                self.pg_extractor.extract_filmworks_by_modified_persons(
                    person_ids,
                    connection,
                )
            )
            for fw_block in filmworks:
                fw_ids = self.schemas_to_ids(fw_block)
                filmworks = self.pg_extractor.extract_filmwork_data(
                    fw_ids,
                    connection,
                )
                self.es_loader.insert_bulk_data(filmworks)

            return last_person

        return PGObject(id=uuid.UUID(last_id), modified_at=last_modified)

    def filmworks_by_modified_genres(
        self, last_modified: datetime, last_id: str, connection
    ) -> PGObject:
        genres = self.pg_extractor.extract_modified_genres(
            last_modified,
            last_id,
            connection,
        )

        if genres:
            last_genre = genres[-1]
            genre_ids = self.schemas_to_ids(genres)
            filmworks = (
                self.pg_extractor.extract_filmworks_by_modified_genres(
                    genre_ids,
                    connection,
                )
            )
            for fw_block in filmworks:
                fw_ids = self.schemas_to_ids(fw_block)
                filmworks = self.pg_extractor.extract_filmwork_data(
                    fw_ids,
                    connection,
                )
                self.es_loader.insert_bulk_data(filmworks)

            return last_genre

        return PGObject(id=uuid.UUID(last_id), modified_at=last_modified)
    
    def modified_filmworks(
        self, last_modified: datetime, last_id: str, connection
    ) -> PGObject:
        
        filmworks = self.pg_extractor.extract_modified_filmworks(
            last_modified,
            last_id,
            connection,
        )

        if filmworks:
            last_filmwork = filmworks[-1]
            fw_ids = self.schemas_to_ids(filmworks)
            filmworks = self.pg_extractor.extract_filmwork_data(
                fw_ids,
                connection,
            )
            self.es_loader.insert_bulk_data(filmworks)

            return last_filmwork

        return PGObject(id=uuid.UUID(last_id), modified_at=last_modified)


def main():
    pg_extractor = PostgresExtractor(DSN, BLOCK_SIZE)
    es_loader = ESLoader(ES_PARAMS, os.environ.get('ES_INDEX_NAME'))

    etl = ETL(pg_extractor, es_loader)

    while True:
        etl.run()
        time.sleep(PAUSE_DURATION)


if __name__ == '__main__':
    main()
