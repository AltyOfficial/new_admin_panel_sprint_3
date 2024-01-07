import sqlite3
from contextlib import contextmanager


class SQLiteExtractor:
    def __init__(self, dp_path: str, block_size: int) -> None:
        self.dp_path = dp_path
        self.block_size = block_size

    @contextmanager
    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.dp_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def prepare_data(self, obj: dict) -> dict:
        try:
            obj['modified_at'] = obj.pop('updated_at')
            del obj['file_path']
        finally:
            return obj

    def extract_data(self, table_name: str, schema: type) -> list:
        """Extract data from SQLite and return list of dataclasses."""

        query = f"""SELECT * FROM {table_name}"""

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)

            while True:
                data = cursor.fetchmany(self.block_size)
                if not data:
                    cursor.close()
                    break

                parsed_data = []
                for item in data:
                    item = self.prepare_data(dict(item))
                    obj = schema(**item)
                    parsed_data.append(obj)

                yield parsed_data
