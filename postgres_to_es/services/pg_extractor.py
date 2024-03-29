import logging
import uuid
from datetime import datetime

import backoff
import psycopg2
from psycopg2.extras import DictCursor

from utils import queries
from utils.schemas import ESFilmwork, Person, PGObject


psycopg2.extras.register_uuid()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)


class PostgresExtractor:

    def __init__(self, params: dict, block_size: int) -> None:
        self.dsn = params
        self.block_size = block_size
        self.connection = self._open_connection()
    
    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def _open_connection(self):
        return psycopg2.connect(**self.dsn, cursor_factory=DictCursor)
    
    def _close_connection(self):
        return self.connection.close()
    
    def _execute_query(self, query, params, connection):
        cursor = connection.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    
    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def _execute_query_gen(self, query: str, params, connection):
        cursor = connection.cursor()
        cursor.execute(query, params)

        while True:
            data = cursor.fetchmany(self.block_size)
            if not data:
                break
            yield data

    def extract_modified_persons(
        self, last_modified: datetime, last_id: uuid.UUID, connection
    ) -> list:
        """Extract person objects that have been modified."""

        results = self._execute_query(
            queries.get_modified_persons,
            (last_modified, last_modified, last_id, self.block_size),
            connection,
        )

        persons = [PGObject(**person) for person in results]

        if persons:
            logging.info(
                'Extracted %s objects from person table.',
                len(persons),
            )

        return persons

    def extract_modified_genres(
        self, last_modified: datetime, last_id: uuid.UUID, connection
    ) -> list:
        """Extract genre objects that have been modified."""

        results = self._execute_query(
            queries.get_modified_genres,
            (last_modified, last_modified, last_id, self.block_size),
            connection,
        )

        genres = [PGObject(**genre) for genre in results]

        if genres:
            logging.info('Extracted %s objects from genre table.', len(genres))

        return genres

    def extract_modified_filmworks(
        self, last_modified: datetime, last_id: uuid.UUID, connection
    ) -> list:
        """Extract filmwork objects that have been modified."""

        results = self._execute_query(
            queries.get_modified_filmworks,
            (last_modified, last_modified, last_id, self.block_size),
            connection,
        )

        filmworks = [PGObject(**filmwork) for filmwork in results]

        if filmworks:
            logging.info(
                'Extracted %s objects from filmwork table.',
                len(filmworks),
            )

        return filmworks

    def extract_filmworks_by_modified_persons(self, id_list: list, connection) -> list:
        """Extract filmwork objects by modified persons."""

        results = self._execute_query_gen(
            queries.get_filmworks_by_modified_persons,
            (tuple(id_list),),
            connection,
        )

        for data in results:
            filmworks = [PGObject(**filmwork) for filmwork in data]
            logging.info(
                'Extracted %s filmworks related to modified persons.',
                len(filmworks),
            )
            yield filmworks

    def extract_filmworks_by_modified_genres(self, id_list: list, connection) -> list:
        """Extract filmwork objects by modified genres."""

        results = self._execute_query_gen(
            queries.get_filmworks_by_modified_genres,
            (tuple(id_list),),
            connection,
        )

        for data in results:
            filmworks = [PGObject(**filmwork) for filmwork in data]
            logging.info(
                'Extracted %s filmworks related to modified genres.',
                len(filmworks),
            )
            yield filmworks

    def extract_filmwork_data(self, id_list: list, connection) -> list:
        """Extract full data of filmworks with selected filmwork ids."""

        if not id_list:
            return []

        filmworks = self._execute_query(
            queries.get_filmworks,
            (tuple(id_list),),
            connection,
        )

        filmwork_data = []
        for filmwork in filmworks:
            params = dict(filmwork)
            persons = {
                'director': '',
                'actor': [],
                'writer': [],
            }

            if person_list := params.pop('persons'):
                for person in person_list:
                    role = person.get('role')
                    if role == 'director':
                        persons['director'] = person.get('full_name')
                    else:
                        persons[role].append(Person(**person))

            params.update({
                'genre': params.pop('genres'),
                'director': persons['director'],
                'actors_names': [actor.full_name for actor in persons['actor']],
                'writers_names': [
                    writer.full_name for writer in persons['writer']
                ],
                'actors': persons['actor'],
                'writers': persons['writer'],
            })
            filmwork_data.append(ESFilmwork(**params))

        logging.info('Extracted %s full filmworks data.', len(filmworks))

        return filmwork_data
