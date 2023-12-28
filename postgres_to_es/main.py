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
    
    def run(self, last_modified: datetime):
        self.etl_genres(last_modified)
        self.etl_persons(last_modified)
        self.etl_filmworks(last_modified)

    def etl_genres(self, last_modified: datetime):
        fw_ids = self.pg_extractor.get_fw_ids_by_modified_genres(last_modified)
        for block in fw_ids:
            fws = self.pg_extractor.extract_filmworks(list(block))
            for fw_block in fws:
                print(len(fw_block))
                pass
    
    def etl_persons(self, last_modified: datetime):
        fw_ids = self.pg_extractor.get_fw_ids_by_modified_persons(last_modified)
        for block in fw_ids:
            fws = self.pg_extractor.extract_filmworks(list(block))
            for fw_block in fws:
                print(len(fw_block))
                pass
    
    def etl_filmworks(self, last_modified: datetime):
        fw_ids = self.pg_extractor.get_fw_ids_by_modified_filmworks(last_modified)
        for block in fw_ids:
            fws = self.pg_extractor.extract_filmworks(list(block))
            for fw_block in fws:
                print(len(fw_block))
                pass


def main():
    pg_extractor = PostgresExtractor('skip', DSN, BLOCK_SIZE)

    data = state.get_state('last_modified')
    if not data:
        state.set_state('last_modified', str(datetime(2000, 1, 1)))
    data = state.get_state('last_modified')
    
    etl = ETL(BLOCK_SIZE, pg_extractor)
    while True:
        etl.run(data)
        break

if __name__ == '__main__':
    main()
