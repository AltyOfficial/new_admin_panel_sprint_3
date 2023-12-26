



class PostgresExtractor:

    def __init__(self, tables: list, params: dict) -> None:
        self.tables = tables
        self.dsn = params
    
    def extract_data(self, table: str):
        """Extracting last modified data from a table."""