import logging
import os
from datetime import datetime

from pg_extractor import PostgresExtractor
from dotenv import load_dotenv

from state.state import state

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)

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

    def __init__(
            self,
            block_size: int,
            pg_extractor: PostgresExtractor,
        ) -> None:
            """Initialize ETL class."""

            self.block_size = block_size
            self.pg_extractor = pg_extractor
    
    def schemas_to_ids(self, objects: list) -> list:
        """Extract id of each object in list and return list of ids."""

        id_list = [str(obj.id) for obj in objects]
        return id_list
    
    def run(self, last_modified: datetime):
        fw_ids = []
        results = self.get_filmwork_ids_by_modified_persons(last_modified)
        print(results)


        fw_ids += results

        print(fw_ids)

        filmworks = self.pg_extractor.extract_filmwork_data(list(set(fw_ids)))
        print(filmworks)


        self.pg_extractor._close_connection()
    
    def get_filmwork_ids_by_modified_persons(self, last_modified: datetime):
        """"""

        persons = self.pg_extractor.extract_modified_persons(last_modified)
        person_ids = self.schemas_to_ids(persons)
        filmworks = self.pg_extractor.extract_filmworks_by_modified_persons(
            person_ids,
        )
        filmwork_ids = self.schemas_to_ids(filmworks)

        return filmwork_ids
    
    # def etl_persons(self, last_modified: datetime):
    #     fw_ids = self.pg_extractor.get_fw_ids_by_modified_persons(last_modified)
    #     for block in fw_ids:
    #         fws = self.pg_extractor.extract_filmworks(list(block))
    #         for fw_block in fws:
    #             print(len(fw_block))
    #             pass
    
    # def etl_filmworks(self, last_modified: datetime):
    #     fw_ids = self.pg_extractor.get_fw_ids_by_modified_filmworks(last_modified)
    #     for block in fw_ids:
    #         fws = self.pg_extractor.extract_filmworks(list(block))
    #         for fw_block in fws:
    #             print(len(fw_block))
    #             pass


def main():
    pg_extractor = PostgresExtractor(DSN, BLOCK_SIZE)

    data = state.get_state('last_modified')
    if not data:
        state.set_state('last_modified', str(datetime(2000, 1, 1)))
    data = state.get_state('last_modified')
    
    etl = ETL(BLOCK_SIZE, pg_extractor)
    while True:
        etl.run(data)
        break

if __name__ == '__main__':
    # import backoff

    # # Новая функция, которая вызывает execute_query
    # def call_execute_query():
    #     return execute_query()

    # # Декоратор backoff.on_exception для обработки исключений ZeroDivisionError
    # @backoff.on_exception(backoff.expo, ZeroDivisionError)
    # def execute_query():
    #     return 1 / 0  # Генерируем исключение ZeroDivisionError

    # # Теперь вызываем нашу новую функцию, которая вызывает execute_query
    # try:
    #     call_execute_query()
    # except backoff.BaseException as e:
    #     print(f"Caught an exception: {e}")
    main()
