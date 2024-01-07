import logging
import os

from dotenv import load_dotenv
from postgres_saver import PostgresSaver
from sqlite_extractor import SQLiteExtractor

from etl_dataclasses import (
    Filmwork,
    Genre,
    GenreFilmwork,
    Person,
    PersonFilmwork,
)

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)

BLOCK_SIZE = 1000

TABLES = {
    'film_work': Filmwork,
    'person': Person,
    'genre': Genre,
    'genre_film_work': GenreFilmwork,
    'person_film_work': PersonFilmwork,
}

DSN = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', 5432),
    'options': '-c search_path=content'
}


def load_from_sqlite():
    """Основной метод загрузки данных из SQLite в Postgres"""

    sqlite_extractor = SQLiteExtractor('db.sqlite', BLOCK_SIZE)
    postgres_saver = PostgresSaver(DSN, list(TABLES.keys()))

    for table_name, schema_name in TABLES.items():
        DATA_COUNT = 0
        data = sqlite_extractor.extract_data(table_name, schema_name)

        try:
            for block in data:
                message = 'Получение данных из таблицы %s'
                logging.info(message, table_name)
                try:
                    DATA_COUNT += len(block)
                    message = 'Загрузка данных в таблицу %s'
                    logging.info(message, table_name)
                    postgres_saver.save_data(table_name, block)
                    message = 'Суммарно загружено %s записей'
                    logging.info(message, DATA_COUNT)
                except Exception as exc:
                    message = 'Ошибка при загрузке данных в таблицу %s: %s'
                    logging.warning(message, table_name, exc)

        except Exception as exc:
            message = 'Ошибка при получении данных из таблицы %s: %s'
            logging.warning(message, table_name, exc)


if __name__ == '__main__':
    logging.info('Начало выполнения ETL процесса')
    load_from_sqlite()
    logging.info('Завершение ETL процесса')
