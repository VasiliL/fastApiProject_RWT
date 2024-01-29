import psycopg2
from psycopg2 import sql, extras
import json
import re


class _DBConnection:
    # CREDENTIALS_FILE = r'C:\Users\user\PycharmProjects\fastApiProject\my_psql\sql_credentials.json'
    CREDENTIALS_FILE = r'C:\Users\basil\PycharmProjects\fastApiProject_RWT\my_psql\sql_credentials.json'

    def __init__(self, db):
        self.connection = None
        self.db = db
        self.closed = True

    def open_connection(self, db):
        credentials = self.load_credentials()
        self.connection = psycopg2.connect(host=credentials[db]['host'], port=credentials[db]['port']
                                           , database=credentials[db]['database'], user=credentials[db]['user']
                                           , password=credentials[db]['password'])
        self.closed = False

    def load_credentials(self):
        with open(self.CREDENTIALS_FILE, 'r') as f:
            return json.load(f)

    def get_connection(self):
        if self.closed:
            self.open_connection(self.db)
        return self.connection

    def __call__(self, *args, **kwargs):
        return self.get_connection()


class BItable:
    """Defines a table in 'cars' database. Send select, insert, and update to the table."""

    def __init__(self, table_name):
        self.table_name = table_name
        self.conn = _DBConnection('cars').get_connection()
        self.db1c_conn = _DBConnection('db1c').get_connection()
        self.columns = None
        self.unique_key = None

    def __retrieve_database_info(self):
        with (self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as target_cur):
            query_columns_names = sql.SQL('SELECT column_name FROM information_schema.columns where table_name = {'
                                          'table_name}').format(table_name=sql.Literal(self.table_name))
            query_get_unique_key = sql.SQL('SELECT a.attname FROM pg_index i JOIN pg_attribute a '
                                           'ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) '
                                           'WHERE  i.indrelid = {table_name}::regclass'
                                           ).format(table_name=sql.Literal(self.table_name))
            target_cur.execute(query_columns_names)
            self.columns = target_cur.fetchall()
            target_cur.execute(query_get_unique_key)
            self.unique_key = target_cur.fetchone()

    def __retrieve_rows(self):
        with self.db1c_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as source_cur:
            query = sql.SQL('SELECT * FROM {db1c_table_name}').format(
                db1c_table_name=sql.Identifier(self.table_name))
            source_cur.execute(query)
            return source_cur.fetchall()

    def __process_rows(self, rows):
        _reference_pattern = re.compile(r'^_(reference|document)\d+$')
        if re.fullmatch(_reference_pattern, self.table_name):
            updates = [sql.SQL('{column_name} = excluded.{column_name}'
                               ).format(column_name=sql.Identifier(i[0])) for i in self.columns]
            conditions = sql.SQL('r._version < excluded._version')
            query = sql.SQL(
                'insert into {table_name} as r values ({placeholders}) on conflict ({p_key}) do update set {'
                'updates} where {conditions}'
            ).format(table_name=sql.Identifier(self.table_name)
                     , placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in self.columns)
                     , p_key=sql.SQL(', ').join(sql.Identifier(_) for _ in self.unique_key)
                     , updates=sql.SQL(', ').join(updates), conditions=conditions)
        else:
            query = sql.SQL('insert into {table_name} as r values ({placeholders}) on conflict do nothing'
                            ).format(table_name=sql.Identifier(self.table_name)
                                     , placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in self.columns))
        with (self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as target_cur):
            for row in rows:
                target_cur.execute(query, row)

    def db1c_sync(self):
        try:
            self.__retrieve_database_info()
            rows = self.__retrieve_rows()
            self.__process_rows(rows)
        finally:
            self.db1c_conn.close()
            self.conn.commit()


class BIView:
    def __init__(self, view) -> None:
        self.view = view
        self.conn = _DBConnection('cars').get_connection()

    def get_data(self):
        with self.conn:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                query = sql.SQL('SELECT * FROM {view}').format(view=sql.Identifier(self.view))
                cursor.execute(query)
                return cursor.fetchall()


if __name__ == '__main__':
    pass
