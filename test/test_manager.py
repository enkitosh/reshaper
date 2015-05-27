import unittest
import os
import psycopg2
from src.reshaper.db import DB
from src.reshaper.transformers import *
from src.reshaper.manager import Manager
from dotenv import load_dotenv

load_dotenv('.env')

class AuthorTransformer(Transformer):
    name = TransformerField(destination='author_name')
    age  = TransformerField(destination='author_age')

    class Meta:
        source_table = 'author'
        destination_table = 'new_author'

class MovieTransformer(Transformer):
    title = TransformerField()
    author_id = SubTransformerField(
        AuthorTransformer,
        relation_table='movie_author'
    )

    class Meta:
        source_table = 'movie'
        destination_table = 'new_movie'

class TestTransformers(unittest.TestCase):
    """
    Test require database authentication from user
    provided in a .env file.
    Your .env file should be located at the directory
    from where you run these tests and should contain:
        DATABASE_NAME=your-database-name
        DATABASE_USER=your-database-user
        DATABASE_PASSWORD=your-database-password
    """
    @classmethod
    def setUpClass(cls):
        cls.postgres = psycopg2.connect(
            database=os.environ.get('DATABASE_NAME'),
            user=os.environ.get('DATABASE_USER'),
            host='localhost',
            password=os.environ.get('DATABASE_PASSWORD')
        )
        cls.postgres.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        cur = cls.postgres.cursor()

        # Create source database
        cur.execute(
            """CREATE DATABASE test_a_db"""
        )

        # Create destination database
        cur.execute(
            """CREATE DATABASE test_b_db"""
        )

        # ============ DB Connections ===============
        cls.source_db = DB(
            dbName="test_a_db",
            dbUser=os.environ.get('DATABASE_USER'),
            dbPass=os.environ.get('DATABASE_PASSWORD')
        )

        cls.dest_db = DB(
            dbName='test_b_db',
            dbUser=os.environ.get('DATABASE_USER'),
            dbPass=os.environ.get('DATABASE_PASSWORD')
        )
        # ============= Source tables ================
        cur = cls.source_db.cursor()
        cur.execute(
            """ CREATE TABLE author(
                id    serial PRIMARY KEY,
                name  varchar(100),
                age   integer
            )
            """
        )
        cur.execute(
            """ CREATE TABLE movie(
                id       serial PRIMARY KEY,
                title    varchar(50),
                author_id integer REFERENCES author
            )
            """
        )
        # =========== Destination tables ============
        cur = cls.dest_db.cursor()
        cur.execute(
            """ CREATE TABLE new_author(
                id    serial PRIMARY KEY,
                author_name    varchar(100),
                author_age     integer
            )
            """
        )

        cur.execute(
            """ CREATE TABLE new_movie(
                id    serial PRIMARY KEY,
                title varchar(50)
            )
            """
        )
        cur.execute(
            """ CREATE TABLE movie_author(
                id    serial PRIMARY KEY,
                movie_id integer REFERENCES new_movie,
                author_id integer REFERENCES new_author
            )
            """
        )

        # ================ Setup manager ==================
        cls.manager = Manager()
        cls.manager.source_db = cls.source_db
        cls.manager.destination_db = cls.dest_db

    def test_source_to_destination_transformation(self):
        """
        Test transforming a single table without relations
        """

        self.source_db.insert_single(
            'author', {'name': 'Takashi Miike', 'age': '54'}
        )
        self.manager.transform(AuthorTransformer())

        # Expect destination db to have 
        # a row in new_author with the following columns
        row = self.dest_db.get_row_from_field(
            'new_author', 'author_name', 'Takashi Miike'
        )
        self.assertEqual(row.get('author_age'), 54)

    def test_relation_table(self):
        """
        Test db insert where two tables are connected in a relation table
        """
        pk_author = self.source_db.insert_single(
            'author', {'name':'David Cronenberg', 'age':'72'}
        )

        pk_movie = self.source_db.insert_single(
            'movie', {
                'title': 'Cosmopolis', 
                'author_id': pk_author 
            }
        )

        self.manager.transform(MovieTransformer())
        
        # Transformer movie table should now only contain title
        new_movie = self.dest_db.get_row_from_field(
            'new_movie', 'title', 'Cosmopolis'
        )
        self.assertIsNone(new_movie.get('author_id'))

        new_author = self.dest_db.get_row_from_field(
            'new_author', 'author_name', 'David Cronenberg'
        )

        relation = self.dest_db.get_row_from_field(
            'movie_author', 'movie_id', new_movie.get('id')
        )

        self.assertEqual(
            new_author.get('id'), 
            relation.get('author_id')
        )

    @classmethod
    def tearDownClass(cls):
        cls.source_db.conn.close()
        cls.dest_db.conn.close()

        with cls.postgres.cursor() as cur:
            cur.execute(
                """ DROP DATABASE test_a_db """
            )

            cur.execute(
                """ DROP DATABASE test_b_db """
            ) 
