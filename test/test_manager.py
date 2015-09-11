import unittest
from src.reshaper.runner import Runner
from src.reshaper.transformers import *
from test.test_data.sql import DBWrapper
from test.test_data.transformers import *

class TestManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DBWrapper()
        cls.db.connect()
        cls.runner = Runner(
            source_db = cls.db.source_db(),
            destination_db = cls.db.destination_db()
        )

    @classmethod
    def tearDownClass(cls):
        cur = cls.runner.source_db.cursor()
        cur.close()
        cur = cls.runner.destination_db.cursor()
        cur.close()
        cls.db.destroy()


    def test_transform(self):
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
        row = self.runner.destination_db.get_row_from_field(
            'new_author', 'author_name', 'Stephen King'
        )
        # Assert that now author exists in a new table
        # transformed with AuthorTransformer
        self.assertEqual(67, row.get('author_age'))

    def test_resolve_subtransformerfield(self):
        """
        Test resolving a SubtransformerField with unique identifier
        """
        country_pk = self.runner.source_db.insert_single(
            'country', {'name' : 'Iceland'}
        ).get('id')

        director = self.runner.source_db.insert_single(
            'director', {
                'name' : 'Baltasar Kormakur',
                'country_id' : country_pk
            }
        ).get('id')

        new_country_pk = self.runner.destination_db.insert_single(
            'new_country', {'name': 'Iceland'}
        ).get('id')

        self.runner.run(DirectorTransformer())

        results = self.runner.destination_db.get_row_from_field(
            'new_director', 'name', 'Baltasar Kormakur'
        )

        self.assertEqual(
            new_country_pk, 
            results.get('country_id')
        )
