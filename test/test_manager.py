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
        ).get('id')



        pk_movie = self.manager.source_db.insert_single(
            'movie', {
                'title':'IT',
                'author_id': pk_author
            }
        ).get('id')
        self.manager.transform(MovieTransformer())
        row = self.manager.destination_db.get_row_from_field(
            'new_author', 'author_name', 'Stephen King'
        )
        # Assert that now author exists in a new table
        # transformed with AuthorTransformer
        self.assertEqual(67, row.get('author_age'))

    def test_resolve_subtransformerfield(self):
        """
        Test resolving a SubtransformerField with unique identifier
        """
        country_pk = self.manager.source_db.insert_single(
            'country', {'name' : 'Iceland'}
        ).get('id')

        director = self.manager.source_db.insert_single(
            'director', {
                'name' : 'Baltasar Kormakur',
                'country_id' : country_pk
            }
        ).get('id')

        new_country_pk = self.manager.destination_db.insert_single(
            'new_country', {'name': 'Iceland'}
        ).get('id')

        self.manager.transform(DirectorTransformer())

        results = self.manager.destination_db.get_row_from_field(
            'new_director', 'name', 'Baltasar Kormakur'
        )

        self.assertEqual(
            new_country_pk, 
            results.get('country_id')
        )

    def test_resolve_subtransformerfield_with_data(self):
        """
        Test resolving a subtransformerfield where destination row is created from data but not source_db
        """
        self.manager.source_db.insert_single(
            'old_fruits', { 
                'fruit' : 'Banana',
                'owner' : 'Bobby'
            }
        ).get('id')

        self.manager.transform(FruitTransformer())

        owner = self.manager.destination_db.get_row_from_field(
            'fruit_owner', 'name', 'Bobby'
        )

        fruit = self.manager.destination_db.get_row_from_field(
            'new_fruits', 'fruit', 'Banana'
        )

        # Assert that the foreign key of fruit.owner
        # is now the same as the primary key of owner
        owner_pk = owner.get('id')
        owner_fruit_pk = fruit.get('owner_id')
        self.assertEqual(owner_pk, owner_fruit_pk)
