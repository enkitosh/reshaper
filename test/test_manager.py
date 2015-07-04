import unittest
from src.reshaper.manager import Manager
from src.reshaper.transformers import *
from test.test_data.sql import DBWrapper
from test.test_data.transformers import *

class TestManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DBWrapper()
        cls.db.connect()
        cls.manager = Manager(
            source_db = cls.db.source_db(),
            destination_db = cls.db.destination_db()
        )

    @classmethod
    def tearDownClass(cls):
        cur = cls.manager.source_db.cursor()
        cur.close()
        cur = cls.manager.destination_db.cursor()
        cur.close()
        cls.db.destroy()


    def test_transform(self):
        pk_author = self.manager.source_db.insert_single(
            'author', {'name':'Stephen King', 'age': '67'}
        )

        pk_movie = self.manager.source_db.insert_single(
            'movie', {
                'title':'IT',
                'author_id': pk_author
            }
        )
        self.manager.transform(MovieTransformer())
        row = self.manager.destination_db.get_row_from_field(
            'new_author', 'author_name', 'Stephen King'
        )
        # Assert that now author exists in a new table
        # transformed with AuthorTransformer
        self.assertEqual(67, row.get('author_age'))

    def test_resolve_subtransformerfield(self):
        """
        Test resolving a SubtransformerField without passing a transformer
        """
        country_pk = self.manager.source_db.insert_single(
            'country', {'name' : 'Iceland'}
        )

        director = self.manager.source_db.insert_single(
            'director', {
                'name' : 'Baltasar Kormakur',
                'country_id' : country_pk
            }
        )

        # Destination db will have to contain
        # new_country table with the exact same
        # name/pk
        new_country_pk = self.manager.destination_db.insert_single(
            'new_country', {'name': 'Iceland'}
        )

        self.manager.transform(DirectorTransformer())

        results = self.manager.destination_db.get_row_from_field(
            'new_director', 'name', 'Baltasar Kormakur'
        )

        self.assertEqual(
            new_country_pk, 
            results.get('country_id')
        )
