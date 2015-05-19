import psycopg2
import psycopg2.extras

class DB:
    """
    psycopg2 client

    """
    def __init__(self, dbName=None, dbUser=None, dbPass=None):
        try:
            self.conn = psycopg2.connect("dbname='%s' user='%s' password='%s'" % (dbName, dbUser, dbPass))
        except:
            raise Exception(
                'Cannot connect to database: %s , user: %s, password: %s' % (dbName, dbUser, dbPass)
            )

    def cursor(self):
        if self.conn:
            return self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        else:
            raise Exception('Connection to database not established')

    def query(self, query_str):
        with self.cursor() as cur:
            cur.execute(query_str)

    def get_table_rows(self, table):
        with self.cursor() as cur:
            rows = []
            try:
                cur.execute(
                    """ SELECT * FROM %s """ % table
                )
                for row in cur:
                    rows.append(row)
                return rows
            except Exception:
                raise Exception('Query for table %s failed' % table)

    def get_row_from_pk(self, table, pk):
        with self.cursor() as cur:
            try:
                cur.execute(
                    """ SELECT * FROM %s WHERE pk=%s """ % (table, pk)
                )
                return cur.fetchone()
            except Exception:
                raise Exception('Could not query pk: %s from table: %s' % (pk, table))

    def get_table_column(self, table, column):
        with self.cursor() as cur:
            try:
                cur.execute(
                    """SELECT %s FROM %s""" % (column, table)
                )
                return cur.fetchall()
            except:
                raise Exception(
                    'Could not select column %s from table %s' % (column, table)
                )
