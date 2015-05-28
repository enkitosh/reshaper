import psycopg2
import os
from src.reshaper.backends.postgresql import DB
from dotenv import load_dotenv
load_dotenv('.env')

class DBWrapper:
    """ 
    Requires database authentication from user
    provided in a .env file.
    Your .env file should be located at the directory
    from where you run these tests and should contain:
        DATABASE_NAME=your-database-name
        DATABASE_USER=your-database-user
        DATABASE_PASSWORD=your-database-password
    """

    def __init__(self, *args, **kwargs):
        self.conn = None
        self.sdb = None
        self.dest_db = None

    def connect(self):
        self.conn = psycopg2.connect(
            database=os.environ.get('DATABASE_NAME'),
            user=os.environ.get('DATABASE_USER'),
            host='localhost',
            password=os.environ.get('DATABASE_PASSWORD')
        )
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )

        cur = self.conn.cursor()
        # Create source database
        cur.execute(
            """CREATE DATABASE test_a_db"""
        )

        # Create destination database
        cur.execute(
            """CREATE DATABASE test_b_db"""
        )

        self.create_tables()

    def destroy(self):
        self.source_db().conn.close()
        self.destination_db().conn.close()
        cur = self.conn.cursor()
        cur.execute(
            """ DROP DATABASE test_a_db """
        )

        cur.execute(
            """ DROP DATABASE test_b_db """
        )
        self.conn.close()

    def source_db(self):
        if not self.sdb:
            self.sdb = DB(
                dbName="test_a_db",
                dbUser=os.environ.get('DATABASE_USER'),
                dbPass=os.environ.get('DATABASE_PASSWORD')
            )
        return self.sdb

    def destination_db(self):
        if not self.dest_db:
            self.dest_db = DB(
                dbName='test_b_db',
                dbUser=os.environ.get('DATABASE_USER'),
                dbPass=os.environ.get('DATABASE_PASSWORD')
            )
        return self.dest_db

    def create_tables(self):
        cur = self.source_db().cursor()
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

        cur = self.destination_db().cursor()
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

