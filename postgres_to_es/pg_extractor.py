from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor



class PostgresExtractor:

    def __init__(self, tables: list, params: dict) -> None:
        self.tables = tables
        self.dsn = params
    
    def extract_modified_genres(self, last_modified: datetime) -> list:
        """Extracting modified genre objects."""

        query = f"""
            SELECT id, modified_at
            FROM content.genre
            WHERE modified_at > %s::date
            ORDER BY modified_at
            LIMIT 10
        """

        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (last_modified,))
            results = cursor.fetchall()
            cursor.close()

        modified_filmworks_ids = []
        modified_genres_ids = []
        for item in results:
            item = dict(item)
            modified_genres_ids.append(item['id'])
        
        query = f"""
            SELECT fw.id, fw.modified_at
            FROM content.filmwork fw
            LEFT JOIN content.genre_filmwork gfw ON gfw.filmwork_id = fw.id 
            WHERE gfw.genre_id IN %s
            LIMIT 10
        """
        if modified_genres_ids:
            with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
                cursor = conn.cursor()
                cursor.execute(query, (tuple(modified_genres_ids),))
                results = cursor.fetchall()
                cursor.close()

            for item in results:
                item = dict(item)
                modified_filmworks_ids.append(item['id'])

        return modified_filmworks_ids
    
    def extract_filmwork_data(self, modified_ids: list):
        """Extracting modified filmwork full data."""

        query = f"""
            SELECT
                fw.id, 
                fw.title, 
                fw.description, 
                fw.rating, 
                fw.type, 
                fw.created_at, 
                fw.modified_at,
                JSON_AGG(
                    json_build_object(
                        'id', p.id,
                        'full_name', p.full_name,
                        'role', pfw.role
                    )
                ) FILTER (WHERE pfw.role = 'PR') as persons,
                ARRAY_AGG(DISTINCT g.name) as genres
            FROM content.filmwork fw
            LEFT JOIN content.person_filmwork pfw ON pfw.filmwork_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_filmwork gfw ON gfw.filmwork_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.title = 'Star Slammer'
            GROUP BY fw.id
            LIMIT 10;
        """

        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
        
        for item in results:
            item = dict(item)
            print(item['title'], item['genres'], item['persons'])
        
        # print(results)

    def extract_data(self, table: str):
        """Extracting last modified data from a table."""

        query = f"""
        SELECT id, full_name, modified_at
        FROM content.person
        ORDER BY modified_at
        LIMIT 10
        """

        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()

        person_ids = []
        for item in results:
            item = dict(item)
            person_ids.append(item['id'])
        
        query = f"""
        SELECT fw.id, fw.modified_at
        FROM content.filmwork fw
        LEFT JOIN content.person_filmwork pfw ON pfw.filmwork_id = fw.id 
        WHERE pfw.person_id IN {tuple(person_ids)}
        LIMIT 10
        """

        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
        
        filmwork_ids = []
        for item in results:
            item = dict(item)
            filmwork_ids.append(item['id'])
        
        query = f"""
        SELECT DISTINCT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created_at,
            fw.modified_at,
            ARRAY_AGG(g.name) AS genres
        FROM content.filmwork fw
        LEFT JOIN content.person_filmwork pfw ON pfw.filmwork_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_filmwork gfw ON gfw.filmwork_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN {tuple(filmwork_ids)}
        GROUP BY fw.id;
        """

        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (filmwork_ids,))
            results = cursor.fetchall()
            cursor.close()
        
        query = f"""
        SELECT 
            p.id,
            p.full_name,
            pfw.role
        FROM content.person p
        LEFT JOIN content.person_filmwork pfw ON pfw.person_id = p.id
        WHERE pfw.filmwork_id IN {tuple(filmwork_ids)} AND pfw.role = 'DR'
        """

        with psycopg2.connect(**self.dsn, cursor_factory=DictCursor) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (filmwork_ids,))
            results = cursor.fetchall()
            cursor.close()
        
        print(results)
