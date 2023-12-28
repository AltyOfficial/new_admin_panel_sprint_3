import logging
import os
from datetime import datetime

from pg_extractor import PostgresExtractor
from dotenv import load_dotenv

from state.state import state

load_dotenv()

# TABLES = {
#     'film_work': Filmwork,
#     'person': Person,
#     'genre': Genre,
#     'genre_film_work': GenreFilmwork,
#     'person_film_work': PersonFilmwork,
# }


BLOCK_SIZE = 100

DSN = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', 5432),
    'options': '-c search_path=content'
}


class ETL:

    def __init__(self, pg_params: dict, block_size: int):
        self.pg_extractor = PostgresExtractor(
            'skip',
            pg_params,
            block_size,
        )
    
    def run(self, last_modified: datetime):
        pass

    def etl_genres(self, last_modified: datetime):
        fw_ids = self.pg_extractor.get_fw_ids_by_modified_genres(last_modified)
        for block in fw_ids:
            fws = self.pg_extractor.extract_filmworks(list(block))
            for fw_block in fws:
                # Upload to ES.
                pass


def main():
    pg_extractor = PostgresExtractor('skip', DSN, BLOCK_SIZE)

    data = state.get_state('last_modified')
    if not data:
        state.set_state('last_modified', str(datetime(2000, 1, 1)))

    last_modified = state.get_state('last_modified')
    fw_ids = pg_extractor.get_fw_ids_by_modified_genres(last_modified)
    for block in fw_ids:
        filmworks = pg_extractor.extract_filmworks(list(block))
        for fw_block in filmworks:
            # here will be upload to ES.
            pass


if __name__ == '__main__':
    main()
