import unittest

from src.reshaper.transformers import *
from test.test_data.transformers import *

class TestTransformers(unittest.TestCase):
    def test_running_transformations(self):
        transformer = MovieTransformer()
        data = {
            'title' : 'The Holy Mountain',
            'author_id': '1'
        }
        transformed = transformer.run_transformations(data)

        title = transformed.get('title')
        author = transformed.get('author_id')
        # Assert that the destination name for this column
        # is author_id
        self.assertEqual(author['data']['key'], 'author_id')

        # Fields
        self.assertIsInstance(
            title.get('field'),
            TransformerField
        )

        self.assertIsInstance(
            author.get('field'),
            RelationTransformerField
        )


