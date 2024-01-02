get_modified_persons = """
    SELECT DISTINCT id, modified_at
    FROM content.person
    WHERE (modified_at > %s OR (modified_at = %s AND id > %s))
    ORDER BY modified_at, id
    LIMIT %s;
"""

get_modified_genres = """
    SELECT DISTINCT id, modified_at
    FROM content.genre
    WHERE (modified_at > %s OR (modified_at = %s AND id > %s))
    ORDER BY modified_at, id
    LIMIT %s;
"""

get_modified_filmworks = """
    SELECT DISTINCT id, modified_at
    FROM content.filmwork
    WHERE (modified_at > %s OR (modified_at = %s AND id > %s))
    ORDER BY modified_at, id
    LIMIT %s;
"""

get_filmworks_by_modified_persons = """
    SELECT DISTINCT fw.id, fw.modified_at
    FROM content.filmwork fw
    LEFT JOIN content.person_filmwork pfw ON pfw.filmwork_id = fw.id 
    WHERE pfw.person_id IN %s;
"""

get_filmworks_by_modified_genres = """
    SELECT DISTINCT fw.id, fw.modified_at
    FROM content.filmwork fw
    LEFT JOIN content.genre_filmwork gfw ON gfw.filmwork_id = fw.id 
    WHERE gfw.genre_id IN %s;
"""

get_filmworks = """
    SELECT
        fw.id, 
        fw.title, 
        fw.description, 
        fw.rating AS imdb_rating, 
        fw.type, 
        fw.created_at, 
        fw.modified_at,
        (
            SELECT JSON_AGG(
                json_build_object(
                    'id', p.id,
                    'full_name', p.full_name,
                    'role', pfw.role
                )
            ) 
            FROM content.person_filmwork pfw
            LEFT JOIN content.person p ON p.id = pfw.person_id
            WHERE pfw.filmwork_id = fw.id
        ) as persons,
        ARRAY_REMOVE(ARRAY_AGG(g.name), NULL) AS genres
    FROM content.filmwork fw
    LEFT JOIN content.genre_filmwork gfw ON gfw.filmwork_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.id IN %s
    GROUP BY fw.id;
"""
