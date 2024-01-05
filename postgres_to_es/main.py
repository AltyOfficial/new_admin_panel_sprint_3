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

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)

BLOCK_SIZE = 100

PAUSE_DURATION = 10

DSN = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', 5432),
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

        # last modified states
        last_modified_person = state.get_state('last_modified_person')
        last_modified_genre = state.get_state('last_modified_genre')
        last_modified_filmwork = state.get_state('last_modified_filmwork')

        # last proccessed id states
        last_person_id = state.get_state('last_person_id')
        last_genre_id = state.get_state('last_genre_id')
        last_filmwork_id = state.get_state('last_filmwork_id')

        fw_ids = []

        results, last_person = self.get_filmwork_ids_by_modified_persons(
            last_modified_person,
            last_person_id,
        )
        fw_ids += results

        results, last_genre = self.get_filmwork_ids_by_modified_genres(
            last_modified_genre,
            last_genre_id,
        )
        fw_ids += results

        results, last_filmwork = self.get_modified_filmwork_ids(
            last_modified_filmwork,
            last_filmwork_id,
        )
        fw_ids += results

        filmworks = self.pg_extractor.extract_filmwork_data(list(set(fw_ids)))

        self.es_loader.create_index()
        results = self.es_loader.insert_bulk_data(filmworks)

        # Rewrite last modified states
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

        logging.info('ETL proccess stopped.')

    def get_filmwork_ids_by_modified_persons(
        self, last_modified: datetime, last_id: str
    ) -> (list, PGObject):
        """Get filmwork ids by persons that have been modified."""

        persons = self.pg_extractor.extract_modified_persons(
            last_modified,
            last_id,
        )
        if persons:
            last_person = persons[-1]
            person_ids = self.schemas_to_ids(persons)
            filmworks = (
                self.pg_extractor.extract_filmworks_by_modified_persons(
                    person_ids,
                )
            )
            filmwork_ids = self.schemas_to_ids(filmworks)

            return filmwork_ids, last_person

        return [], PGObject(id=uuid.UUID(last_id), modified_at=last_modified)

    def get_filmwork_ids_by_modified_genres(
        self, last_modified: datetime, last_id: str
    ) -> (list, PGObject):
        """Get filmwork ids by genres that have been modified."""

        genres = self.pg_extractor.extract_modified_genres(
            last_modified,
            last_id,
        )
        if genres:
            last_genre = genres[-1]
            genre_ids = self.schemas_to_ids(genres)
            filmworks = self.pg_extractor.extract_filmworks_by_modified_genres(
                genre_ids,
            )
            filmwork_ids = self.schemas_to_ids(filmworks)

            return filmwork_ids, last_genre

        return [], PGObject(id=uuid.UUID(last_id), modified_at=last_modified)

    def get_modified_filmwork_ids(
        self, last_modified: datetime, last_id: str
    ) -> (list, PGObject):
        """Return list of modified filmwork ids."""

        filmworks = self.pg_extractor.extract_modified_filmworks(
            last_modified,
            last_id,
        )

        if filmworks:
            last_filmwork = filmworks[-1]
            filmwork_ids = self.schemas_to_ids(filmworks)

            return filmwork_ids, last_filmwork

        return [], PGObject(id=uuid.UUID(last_id), modified_at=last_modified)


def main():
    pg_extractor = PostgresExtractor(DSN, BLOCK_SIZE)
    es_loader = ESLoader(ES_PARAMS, os.environ.get('ES_INDEX_NAME'))

    etl = ETL(pg_extractor, es_loader)

    while True:
        etl.run()
        time.sleep(PAUSE_DURATION)


if __name__ == '__main__':
    main()
