from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from utils.schemas import ESFilmwork, Person
from utils import queries


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
    
    def get_fw_ids_by_modified_genres(self, last_modified: datetime) -> list:
        """Extracting modified genre objects."""

        modified_genres = self.execute_query(
            queries.extract_modified_genres,
            (last_modified,),
        )

        for genres_block in modified_genres:
            modified_filmworks_ids = []
            modified_genres_ids = []
            for genre in genres_block:
                modified_genres_ids.append(dict(genre).get('id'))
            
            if modified_genres_ids:
                modified_filmworks = self.execute_query(
                    queries.extract_modified_filmworks_by_genres,
                    (tuple(modified_genres_ids),),
                )
                for filmworks_block in modified_filmworks:
                    for item in filmworks_block:
                        modified_filmworks_ids.append(dict(item).get('id'))
                    
                    yield modified_filmworks_ids

    def extract_modified_persons(self, last_modified: datetime) -> list:
        """Extracting modified person objects."""

        pass
    
    def extract_filmworks(self, modified_ids: list):
        """Extracting modified filmwork full data."""

        modified_filmworks = self.execute_query(
            queries.extract_modified_filmworks,
            (tuple(modified_ids),)
        )

        for filmworks_block in modified_filmworks:

            print(filmworks_block)
        
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
                    'actors_names': ', '.join([actor.full_name for actor in persons['AC']]),
                    'writers_names': ', '.join([writer.full_name for writer in persons['PR']]),
                    'actors': persons['AC'],
                    'writers': persons['PR'],
                })
                obj = ESFilmwork(**filmwork)
                filmworks.append(obj)
            
            yield filmworks
