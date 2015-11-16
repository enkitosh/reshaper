import unittest

from mockredis.client import MockRedis

from src.reshaper.runner import Runner
from test.test_data.sql import DBWrapper
from test.test_data.transformers import *

class TestCache(unittest.TestCase):

    def setUp(self):
        self.db = DBWrapper()
        self.db.connect()
        self.runner = Runner(
            source_db = self.db.source_db(),
            destination_db = self.db.destination_db(),
			cache=True
        )

    def tearDown(self):
        cur = self.runner.source_db.cursor()
        cur.close()
        cur = self.runner.destination_db.cursor()
        cur.close()
        self.db.destroy()


    def test_running_with_cache(self):
        self.runner.cache = MockRedis()
        pk_author = self.runner.source_db.insert_single(
            'author', {'name':'Stephen King', 'age': '67'}
        ).get('id')

        pk_movie = self.runner.source_db.insert_single(
            'movie', {
                'title':'IT',
                'author_id': pk_author
            }
        ).get('id')
        self.runner.run(MovieTransformer())
	
        # Verify that runners cache has transformer last index with namespace
        lsi = self.runner.cache.get('MovieTransformer_last_source_index').decode('utf-8')
        self.assertEqual('1', lsi)
        
        # Add more records
        pk_author = self.runner.source_db.insert_single(
            'author', {'name':'Stephen King', 'age': '67'}
        ).get('id')

        pk_movie = self.runner.source_db.insert_single(
            'movie', {
                'title':'IT',
                'author_id': pk_author
            }
        ).get('id')
        count = self.runner.run(MovieTransformer())

        # Verify that only a single object was transformed
        self.assertEqual(1, count)

        # Verify that now the last source index is 2
        lsi = self.runner.cache.get('MovieTransformer_last_source_index').decode('utf-8')
        self.assertEqual('2', lsi)
	
