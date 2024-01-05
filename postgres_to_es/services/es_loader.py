import json
import logging
import os

import backoff
from elasticsearch import Elasticsearch, helpers

logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s:%(levelname)s - %(message)s'
)


class ESLoader:

    def __init__(self, params: dict, index_name: str):
        self.es = Elasticsearch(params)
        self.index_name = index_name

    @backoff.on_exception(wait_gen=backoff.expo, exception=ConnectionError)
    def create_index(self):
        """Create ElasticSearch index."""

        index_name = self.index_name
        index_path = 'utils/es_movies_index.json'

        if (
            os.path.exists(index_path) and
            not self.es.indices.exists(index=index_name)
        ):
            index_name = self.index_name
            with open(index_path, 'r') as file:
                body = json.load(file)
                self.es.indices.create(
                    index=index_name,
                    mappings=body['mappings'],
                    settings=body['settings'],
                )

            logging.info('ElasticSearch %s index created.', index_name)

    @backoff.on_exception(wait_gen=backoff.expo, exception=ConnectionError)
    def insert_bulk_data(self, data: list):
        """Insert data into ElasticSearch index."""

        actions = []
        for item in data:
            item_dict = {
                'id': item.id,
                'imdb_rating': item.imdb_rating,
                'genre': item.genre,
                'title': item.title,
                'description': item.description,
                'director': item.director,
                'actors_names': item.actors_names,
                'writers_names': item.writers_names,
                'actors': [{
                    'id': person.id,
                    'name': person.full_name,
                } for person in item.actors],
                'writers': [{
                    'id': person.id,
                    'name': person.full_name,
                } for person in item.writers],
            }
            action = {
                '_id': item.id,
                '_index': 'movies',
                '_source': item_dict
            }
            actions.append(action)

        try:
            helpers.bulk(self.es, actions)
            logging.error('Loaded %s objects to ES index.', len(data))
        except Exception as exc:
            logging.error('Could not load data to ES: %s.', exc)
