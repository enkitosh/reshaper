import psycopg2
import psycopg2.extras

class DB:
    """
    psycopg2 client

    """
    def __init__(self, dbName=None, dbUser=None, dbPass=None):
        try:
            self.conn = psycopg2.connect(
                "dbname='%s' user='%s' password='%s'" % (dbName, dbUser, dbPass)
            )
        except:
            raise Exception(
                'Cannot connect to database: %s , user: %s, password: %s' % (dbName, dbUser, dbPass)
            )

    def cursor(self):
        if self.conn:
            return self.conn.cursor(
                cursor_factory = psycopg2.extras.RealDictCursor)
        else:
            raise Exception('Connection to database not established')

    def get_table_rows(self, table):
        """
        Get all rows of a table in database
        :param str table: Name of table in database
        :return: A list of rows 
        """
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
        """
        Fetch a row from database table based on id
        :param str table: Name of table in database
        :param str pk: id of row in database
        :return: A dictionary with column name:value from the row containing the id passed in
        """
        with self.cursor() as cur:
            try:
                cur.execute(
                    """ SELECT * FROM %s WHERE id=%s """ % (table, pk)
                )
                return cur.fetchone()
            except Exception:
                raise Exception('Could not query id: %s from table: %s' % (pk, table))
    
    def get_row_from_field(self, table, field_name, value):
        """
        Gets a row from table where field is equal to value.
        This function will fail as soon as there are more then one value
        that could match the value passed in. If fetching a single vaue
        from a primary key use get_row_from_pk instead
        """
        with self.cursor() as cur:
            try:
                cur.execute(""" SELECT * FROM %s WHERE %s='%s'""" % (table, field_name, value))
                return cur.fetchone()
            except Exception:
                raise Exception("get_row_from_field: query error")

    def get_pk_from_field(self, table, field_name, value):
        """
        Return the primary key where field value matches
        """
        return self.get_row_from_field(table, field_name, value).get('pk')

    def build_single(self, table, values):
        """
        Build a SQL query out of a single dictionary

        :param str values: Name of table in database
        :param dict values: Dictionary to build SQL query with
        """
        build = 'INSERT INTO %s ' % table
        stub_a = '('
        stub_b = '('
        for key, value in values.items():
            stub_a += key + ','
            stub_b += "'" + str(value) + "',"
        stub_a = stub_a[:-1] + ')'
        stub_b = stub_b[:-1] + ')'
        build  += stub_a
        build  += ' VALUES %s' % stub_b
        return build

    def build_many(self, table, values):
        """
        Build s SQL which enables bulk loading values from 
        a list of dictionaries

        :param str table: Name of table in database
        :param list values: List of dictionaries
        """

        # We need a single set of keys so 
        # we select the first dictionary
        vals = values[0]
        build = 'INSERT INTO %s' % table
        build += '('
        value_part = '('
        for key in vals.keys():
            build += key + ','
            value_part += '%'
            value_part += '(%s)s,' % key
        value_part = value_part[:-1] + ')'
        build = build[:-1] + ')'
        build += ' VALUES %s' % value_part
        return build

    def insert_single(self, table, row):
        """
        Insert a single row

        :param str table: Name of db table to insert values into
        :param dict row: Dictionary containing values to insert
        :return: pk of the row inserted
        """
        build  = self.build_single(table, row)
        build += ' RETURNING id'
        with self.cursor() as cur:
            try:
                cur.execute(build)
                return cur.fetchone().get('id')
            except Exception:
                raise Exception('Could not insert data')

    def insert_many(self, table, rows):
        """
        Insert multiple rows into database

        :param str table: Name of table in database
        :param list rows: List of dictionaries with values to insert into each row
        """
        build_query = self.build_many(table, rows)
        with self.cursor() as cur:
            try:
                cur.executemany(build_query, rows)
            except Exception:
                raise Exception('Could not bulk insert')

    def add_relation(self, table, pk_rel, pk_trans):
        with self.cursor() as cur:
            try:
                cur.execute(""" INSERT INTO %s VALUES(%s, %s) """ % (table, pk_rel, pk_trans))
            except Exception:
                raise Exception('Failed to add relation')
