from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from utils.schemas import ESFilmwork, Person
from utils import queries

import logging


logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)


class PostgresExtractor:

    def __init__(self, tables: list, params: dict, block_size: int) -> None:
        self.tables = tables
        self.dsn = params
        self.block_size = block_size
    
    def execute_query(self, query: str, params=None):
        """Execute query with optional params."""
    
        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)

            while True:
                results = cursor.fetchmany(self.block_size)
                if not results:
                    cursor.close()
                    break
                
                yield results
    
    def extract_filmwork_ids(self, objects, table_name):

        query = {
            'genres': queries.extract_modified_filmworks_by_genres,
            'persons': queries.extract_modified_filmworks_by_persons,
        }

        for block in objects:
            message = 'Extracted %s objects from %s.'
            logging.info(message, len(block), table_name)

            modified_objects_ids = []
            modified_filmworks_ids = []
            for object in block:
                modified_objects_ids.append(dict(object).get('id'))
            
            if modified_objects_ids:
                modified_filmworks = self.execute_query(
                    query[table_name],
                    (tuple(modified_objects_ids),),
                )

                for fw_block in modified_filmworks:
                    message = 'Extracted %s filmworks.'
                    logging.info(message, len(fw_block))

                    for fw in fw_block:
                        modified_filmworks_ids.append(dict(fw).get('id'))
                    
                    yield modified_filmworks_ids
    
    def get_fw_ids_by_modified_genres(self, last_modified: datetime) -> list:
        """Get filmwork id list that have modified genre objects."""

        modified_genres = self.execute_query(
            queries.extract_modified_genres,
            (last_modified,),
        )

        result = self.extract_filmwork_ids(modified_genres, 'genres')
        for fw_ids in result:
            yield fw_ids
    
    def get_fw_ids_by_modified_persons(self, last_modified: datetime) -> list:
        """Get filmwork id list that have modified person objects."""

        modified_persons = self.execute_query(
            queries.extract_modified_persons,
            (last_modified,),
        )

        result = self.extract_filmwork_ids(modified_persons, 'persons')
        for fw_ids in result:
            yield fw_ids
    
    def get_fw_ids_by_modified_filmworks(self, last_modified: datetime) -> list:
        """Get modified filmwork ids."""

        modified_filmworks = self.execute_query(
            queries.extract_modified_filmworks_by_filmworls,
            (last_modified,),
        )

        for block in modified_filmworks:
            message = 'Extracted %s objects from filmworks.'
            logging.info(message, len(block))
            modified_filmwork_ids = []
            for object in block:
                modified_filmwork_ids.append(dict(object).get('id'))
            
            yield modified_filmwork_ids

    
    def extract_filmworks(self, modified_ids: list):
        """Extracting modified filmwork full data."""

        modified_filmworks = self.execute_query(
            queries.extract_modified_filmworks,
            (tuple(modified_ids),)
        )

        for filmworks_block in modified_filmworks:
            filmworks = []
            for filmwork in filmworks_block:
                filmwork = dict(filmwork)
                persons = {
                    'DR': None,
                    'AC': [],
                    'PR': [],
                }
                if person_list := filmwork.pop('persons'):
                    for person in person_list:
                        role = person.get('role')
                        if role == 'DR':
                            persons['DR'] = person.get('full_name')
                        else:
                            persons[role].append(Person(**person))
                filmwork.update({
                    'genre': ', '.join(filmwork.pop('genres')),
                    'director': persons['DR'],
                    'actors_names': ', '.join(
                        [actor.full_name for actor in persons['AC']]
                    ),
                    'writers_names': ', '.join(
                        [writer.full_name for writer in persons['PR']]
                    ),
                    'actors': persons['AC'],
                    'writers': persons['PR'],
                })
                obj = ESFilmwork(**filmwork)
                filmworks.append(obj)
            
            yield filmworks
