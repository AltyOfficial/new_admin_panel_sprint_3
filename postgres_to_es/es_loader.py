from elasticsearch import Elasticsearch, helpers

from utils.es_index import es_index

import backoff



class ESLoader:

    def __init__(self, index_name: str):
        self.base_url = 'http://127.0.0.1:9200'
        self.index_name = index_name
        self.es = Elasticsearch([{'scheme': 'http', 'host': 'localhost', 'port': 9200}])
    
    @backoff.on_exception(wait_gen=backoff.expo, exception=ConnectionError)
    def create_schema(self):
        index_name = 'movies'
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(
                index=index_name,
                mappings=es_index['mappings'],
                settings=es_index['settings'],
            )
    
    @backoff.on_exception(wait_gen=backoff.expo, exception=ConnectionError)
    def insert_bulk_data(self, data: list):
        actions = []
        for item in data:
            item_dict = {
                "id": item.id,
                "imdb_rating": item.imdb_rating,
                "genre": item.genre,
                "title": item.title,
                "description": item.description,
                "director": item.director,
                "actors_names": item.actors_names,
                "writers_names": item.writers_names,
                "actors": [{"id": person.id, "name": person.full_name} for person in item.actors],
                "writers": [{"id": person.id, "name": person.full_name} for person in item.writers],
            }
            action = {
                '_id': item.id,
                "_index": 'movies',
                "_source": item_dict
            }
            actions.append(action)

        helpers.bulk(self.es, actions)

