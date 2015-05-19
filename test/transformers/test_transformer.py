from src.reshaper.db import DB
from src.reshaper.transformers import *

def hello(str):
    return str + ', nice to see you :)'

def done(str):
    print('Transformation done')

class TransformerTest(Transformer):
    title = TransformerField(
        destination='new_title',
        filters=[hello],
        actions=[done]
    )

    class Meta:
        source = 'test_table'
        destination = 'new_table'

def test_run():
    db_conn = DB(
        dbName='reshaper', 
        dbUser='postgres',
        dbPass='feedbin'
    )
    transformer = TransformerTest()
    transformer.conn = db_conn
    print(transformer.transform())

