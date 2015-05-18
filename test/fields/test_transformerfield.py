import unittest
from src.reshaper.fields import TransformerField

class TransformerFieldTest(unittest.TestCase):
    def test_nothing(self):
        tf = TransformerField('default')
        self.assertEqual('default', tf.identifier)
