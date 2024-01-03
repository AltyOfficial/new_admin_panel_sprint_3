import logging
import uuid
from datetime import datetime

import backoff
import psycopg2
from psycopg2.extras import DictCursor

from utils import queries
from utils.schemas import ESFilmwork, Person, PGObject


psycopg2.extras.register_uuid()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)


class PostgresExtractor:

    def __init__(self, params: dict, block_size: int) -> None:
        self.dsn = params
        self.block_size = block_size

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def _execute_query(self, query: str, params=None):
        """Execute query and return results."""

        with psycopg2.connect(
            **self.dsn, cursor_factory=DictCursor
        ) as conn, conn.cursor() as curs:

            curs.execute(query, params)
            return curs.fetchall()

    def extract_modified_persons(
        self, last_modified: datetime, last_id: uuid.UUID
    ) -> list:
        """Extract person objects that have been modified."""

        results = self._execute_query(
            queries.get_modified_persons,
            (last_modified, last_modified, last_id, self.block_size),
        )

        persons = [PGObject(**person) for person in results]

        if persons:
            logging.info(
                'Extracted %s objects from person table.',
                len(persons),
            )

        return persons

    def extract_modified_genres(
        self, last_modified: datetime, last_id: uuid.UUID
    ) -> list:
        """Extract genre objects that have been modified."""

        results = self._execute_query(
            queries.get_modified_genres,
            (last_modified, last_modified, last_id, self.block_size),
        )

        genres = [PGObject(**genre) for genre in results]

        if genres:
            logging.info('Extracted %s objects from genre table.', len(genres))

        return genres

    def extract_modified_filmworks(
        self, last_modified: datetime, last_id: uuid.UUID
    ) -> list:
        """Extract filmwork objects that have been modified."""

        results = self._execute_query(
            queries.get_modified_filmworks,
            (last_modified, last_modified, last_id, self.block_size),
        )

        filmworks = [PGObject(**filmwork) for filmwork in results]

        if filmworks:
            logging.info(
                'Extracted %s objects from filmwork table.',
                len(filmworks),
            )

        return filmworks

    def extract_filmworks_by_modified_persons(self, id_list: list) -> list:
        """Extract filmwork objects by modified persons."""

        results = self._execute_query(
            queries.get_filmworks_by_modified_persons,
            (tuple(id_list),),
        )

        filmworks = [PGObject(**filmwork) for filmwork in results]

        if filmworks:
            logging.info(
                'Extracted %s filmworks related to modified persons.',
                len(filmworks),
            )

        return filmworks

    def extract_filmworks_by_modified_genres(self, id_list: list) -> list:
        """Extract filmwork objects by modified genres."""

        results = self._execute_query(
            queries.get_filmworks_by_modified_genres,
            (tuple(id_list),),
        )

        filmworks = [PGObject(**filmwork) for filmwork in results]

        if filmworks:
            logging.info(
                'Extracted %s filmworks related to modified persons.',
                len(filmworks),
            )

        return filmworks

    def extract_filmwork_data(self, id_list: list) -> list:
        """Extract full data of filmworks with selected filmwork ids."""

        if not id_list:
            return []

        filmworks = self._execute_query(
            queries.get_filmworks,
            (tuple(id_list),),
        )

        filmwork_data = []
        for filmwork in filmworks:
            params = dict(filmwork)
            persons = {
                'DR': None,
                'AC': [],
                'PR': [],
            }

            if person_list := params.pop('persons'):
                for person in person_list:
                    role = person.get('role')
                    if role == 'DR':
                        persons['DR'] = person.get('full_name')
                    else:
                        persons[role].append(Person(**person))

            params.update({
                'genre': ', '.join(params.pop('genres')),
                'director': persons['DR'],
                'actors_names': ', '.join(
                    [actor.full_name for actor in persons['AC']],
                ),
                'writers_names': ', '.join(
                    [writer.full_name for writer in persons['PR']],
                ),
                'actors': persons['AC'],
                'writers': persons['PR'],
            })
            filmwork_data.append(ESFilmwork(**params))

        logging.info('Extracted %s full filmworks data.', len(filmworks))

        return filmwork_data
