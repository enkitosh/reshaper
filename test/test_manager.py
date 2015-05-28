import unittest
from src.reshaper.manager import Manager
from test.test_data.sql import DBWrapper
from test.test_data.transformers import *

class TestTransformers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # ================ Setup manager ==================
        cls.db = DBWrapper()
        cls.db.connect()
        cls.manager = Manager(
            source_db = cls.db.source_db(),
            destination_db = cls.db.destination_db()
        )

    def test_source_to_destination_transformation(self):
        """
        Test transforming a single table without relations
        """

        self.manager.source_db.insert_single(
            'author', {'name': 'Takashi Miike', 'age': '54'}
        )
        self.manager.transform(AuthorTransformer())

        # Expect destination db to have 
        # a row in new_author with the following columns
        row = self.manager.destination_db.get_row_from_field(
            'new_author', 'author_name', 'Takashi Miike'
        )
        self.assertEqual(row.get('author_age'), 54)

    def test_relation_table(self):
        """
        Test db insert where two tables are connected in a relation table
        """
        pk_author = self.manager.source_db.insert_single(
            'author', {'name':'David Cronenberg', 'age':'72'}
        )

        pk_movie = self.manager.source_db.insert_single(
            'movie', {
                'title': 'Cosmopolis', 
                'author_id': pk_author 
            }
        )

        self.manager.transform(MovieTransformer())
        
        # Transformer movie table should now only contain title
        new_movie = self.manager.destination_db.get_row_from_field(
            'new_movie', 'title', 'Cosmopolis'
        )
        self.assertIsNone(new_movie.get('author_id'))

        new_author = self.manager.destination_db.get_row_from_field(
            'new_author', 'author_name', 'David Cronenberg'
        )

        relation = self.manager.destination_db.get_row_from_field(
            'movie_author', 'movie_id', new_movie.get('id')
        )

        self.assertEqual(
            new_author.get('id'), 
            relation.get('author_id')
        )

    @classmethod
    def tearDownClass(cls):
        cur = cls.manager.source_db.cursor()
        cur.close()
        cur = cls.manager.destination_db.cursor()
        cur.close()
        cls.db.destroy()
