from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection
import backoff

from utils.schemas import ESFilmwork, Person, PGObject
from utils import queries


import logging

psycopg2.extras.register_uuid()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)


class PostgresExtractor:

    def __init__(self, params: dict, block_size: int) -> None:
        self.dsn = params
        self.block_size = block_size
        self.connection = self._connect()

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def _connect(self) -> connection:
        """Connection to PostgreSQL database."""

        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            message = 'Successfully connected to PG database.'
            logging.info(message)

            return conn

    def _close_connection(self) -> None:
        """Closes connection to PostgreSQL database."""

        if self.connection:
            self.connection.close()
            message = 'Connection to PG database is closed'
            logging.info(message)

    def _execute_query(self, query: str, params=None):
        """Execute query and return results."""

        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def extract_modified_persons(self, last_modified: datetime) -> list:
        """Ext"""

        results = self._execute_query(
            queries.get_modified_persons,
            (last_modified, self.block_size),
        )

        persons = [PGObject(**person) for person in results]
        return persons
    
    def extract_filmworks_by_modified_persons(self, id_list: list) -> list:
        """Extract filmwork objects by modified persons."""

        results = self._execute_query(
            queries.get_filmworks_by_modified_persons,
            (tuple(id_list),),
        )

        filmworks = [PGObject(**filmwork) for filmwork in results]
        return filmworks

    def extract_filmwork_data(self, id_list: list) -> list:
        """Extract full data of filmworks by selected ids."""

        if not id_list:
            return []

        filmworks = self._execute_query(
            queries.get_filmworks,
            (tuple(id_list),),
        )

        filmwork_data = []
        for filmwork in filmworks:
            params = dict(filmwork)
            persons = {
                'DR': None,
                'AC': [],
                'PR': [],
            }

            if person_list := params.pop('persons'):
                for person in person_list:
                    role = person.get('role')
                    if role == 'DR':
                        persons['DR'] = person.get('full_name')
                    else:
                        persons[role].append(Person(**person))
            
            params.update({
                'genre': ', '.join(params.pop('genres')),
                'director': persons['DR'],
                'actors_names': ', '.join(
                    [actor.full_name for actor in persons['AC']],
                ),
                'writers_names': ', '.join(
                    [writer.full_name for writer in persons['PR']],
                ),
                'actors': persons['AC'],
                'writers': persons['PR'],
            })
            filmwork_data.append(ESFilmwork(**params))
        
        return filmwork_data
