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



DSN = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', 5432),
    'options': '-c search_path=content'
}


def main():
    data = state.get_state('last_modified')
    if not data:
        state.set_state('last_modified', str(datetime(2000, 1, 1)))
    pg_extractor = PostgresExtractor('skip', DSN)
    # pg_extractor.extract_data('person')
    last_modified = state.get_state('last_modified')
    pg_extractor.extract_modified_genres(last_modified)
    pg_extractor.extract_filmwork_data('test')


if __name__ == '__main__':
    main()
