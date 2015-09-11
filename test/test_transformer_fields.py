import unittest
from src.reshaper.transformers import *

def filter_1(value):
    return '-*%s' % value

def filter_2(value):
    return '%s*-' % value

class TestTransformerFields(unittest.TestCase):
    def test_applying_field_filters(self):
        """
        Test applying filters of a field to a value
        """

        field = Field('test', filters=[filter_1, filter_2])
        
        # Applying filters should have a piping effect
        # on the value where filter_1 alters the value
        # and passes the altered value to filter_2 etc..
        f_value = field.apply_filters('testing')

        # The resulting string should be -*testing*-
        self.assertEqual(f_value, '-*testing*-')
