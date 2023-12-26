import psycopg2
from psycopg2 import sql, extras
import json

CREDENTIALS_FILE = r'C:\Users\user\PycharmProjects\fastApiProject\my_psql\sql_credentials.json'


class _DBConnection:
    @staticmethod
    def load_credentials(credentials_file):
        with open(credentials_file, 'r') as f:
            return json.load(f)

    @classmethod
    def get_connection(cls, db, credentials_file):
        credentials = cls.load_credentials(credentials_file)
        return psycopg2.connect(host=credentials[db]['host'], port=credentials[db]['port']
                                , database=credentials[db]['database'], user=credentials[db]['user']
                                , password=credentials[db]['password'])


class BItable:
    """Defines a table in 'cars' database. Send select, insert, and update to the table."""

    def __init__(self, table_name, conn=None):
        self.__credentials_file = CREDENTIALS_FILE
        self.columns = None
        self.primary = None
        self.db1c_conn = _DBConnection.get_connection('db1c', self.__credentials_file)
        self.table_name = table_name
        self.conn = conn
        if self.conn is None:
            self.conn = _DBConnection().get_connection('cars', self.__credentials_file)
            self.local_conn = True

    def __retrieve_database_info(self):
        with (self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as target_cur):
            query_columns_names = sql.SQL('SELECT column_name FROM information_schema.columns where table_name = {'
                                          'table_name}').format(table_name=sql.Literal(self.table_name))
            query_get_primary = sql.SQL('SELECT a.attname FROM pg_index i JOIN pg_attribute a '
                                        'ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) '
                                        'WHERE  i.indrelid = {table_name}::regclass AND i.indisprimary'
                                        ).format(table_name=sql.Literal(self.table_name))
            target_cur.execute(query_columns_names)
            self.columns = target_cur.fetchall()
            target_cur.execute(query_get_primary)
            self.primary = target_cur.fetchone()

    def __retrieve_rows(self):
        with self.db1c_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as source_cur:
            query = sql.SQL('SELECT * FROM {db1c_table_name}').format(
                db1c_table_name=sql.Identifier(self.table_name))
            source_cur.execute(query)
            return source_cur.fetchall()

    def __process_rows(self, rows):
        if self.table_name.startswith('_reference'):
            updates = [sql.SQL('{column_name} = excluded.{column_name}'
                               ).format(column_name=sql.Identifier(i[0])) for i in self.columns]
            conditions = sql.SQL('r._version < excluded._version')
            query = sql.SQL(
                'insert into {table_name} as r values ({placeholders}) on conflict ({p_key}) do update set {'
                'updates} where {conditions}'
            ).format(table_name=sql.Identifier(self.table_name)
                     , placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in self.columns)
                     , p_key=sql.SQL(', ').join(sql.Identifier(_) for _ in self.primary)
                     , updates=sql.SQL(', ').join(updates), conditions=conditions)
        elif self.table_name.startswith('_inforg'):
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
            if self.local_conn:
                self.conn.close()


if __name__ == '__main__':
    pass
