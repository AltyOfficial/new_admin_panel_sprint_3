from elasticsearch import Elasticsearch, helpers



class ESLoader:

    def __init__(self, index_name: str):
        self.base_url = 'http://127.0.0.1:9200'
        self.index_name = index_name

