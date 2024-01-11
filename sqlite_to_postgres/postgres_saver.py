import io

import psycopg2
from psycopg2.extras import DictCursor


class PostgresSaver:

    def __init__(self, dsn: dict, tables: list[str]) -> None:
        self.dsn = dsn
        self.tables = tables
        self.truncate_tables()

    def truncate_tables(self) -> None:
        for table_name in self.tables:
            table_name = self.prepare_table_name(table_name)

    def execute_query(self, query: str, data=None, is_copy=False) -> None:
        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            cursor = conn.cursor()
            if is_copy:
                cursor.copy_expert(query, data)
            else:
                cursor.execute(query)
            cursor.close()

    def prepare_table_name(self, table_name: str) -> str:
        if 'film_work' in table_name:
            table_name = table_name.replace('film_work', 'filmwork')
        return table_name

    def generate_csv_obj(self, data: list) -> io.StringIO:
        csv_data = io.StringIO()

        for item in data:
            values = list(item._asdict.values())
            parsed_values = [str((lambda i: i or '')(i)) for i in values]
            final_string = '|'.join(parsed_values) + '\n'
            csv_data.write(final_string)
        csv_data.seek(0)

        return csv_data

    def save_data(self, table_name: str, data: list) -> None:
        """Load data to Postgres tables."""

        table_name = self.prepare_table_name(table_name)
        csv_data = self.generate_csv_obj(data)
        query = f"""
            COPY content.{table_name}
            FROM STDIN (
                FORMAT 'csv',
                HEADER false,
                DELIMITER '|',
                ENCODING 'UTF8',
                QUOTE E'\u0007'
            )
        """
        self.execute_query(query, csv_data, True)
