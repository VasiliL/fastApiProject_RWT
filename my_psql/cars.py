import psycopg2
from psycopg2 import sql, extras
import json

CREDENTIALS_FILE = r'C:\Users\user\PycharmProjects\fastApiProject\my_psql\sql_credentials.json'

with open(CREDENTIALS_FILE, 'r') as f:
    credentials = json.load(f)
    credentials_cars = credentials['cars']
    credentials_db1c = credentials['db1c']


class BItable:
    """Defines a table in 'cars' database. Send select, insert, and update to the table."""

    def __init__(self, table_name, conn=None):
        self.table_name = table_name
        self.columns = None
        self.primary = None
        self.db1c_table_name = table_name
        self.conn = conn
        self.host = credentials_cars['host']
        self.port = credentials_cars['port']
        self.database = credentials_cars['database']
        self.schema = credentials_cars['schema']
        self.username = credentials_cars['user']
        self.password = credentials_cars['password']
        self.db1c_conn = None
        self.db1c_host = credentials_db1c['host']
        self.db1c_port = credentials_db1c['port']
        self.db1c_database = credentials_db1c['database']
        self.db1c_schema = credentials_db1c['schema']
        self.db1c_username = credentials_db1c['user']
        self.db1c_password = credentials_db1c['password']

    def connect_cars(self):
        self.conn = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.username,
                                     password=self.password)

    def connect_db1c(self):
        self.db1c_conn = psycopg2.connect(host=self.db1c_host, port=self.db1c_port, database=self.db1c_database,
                                          user=self.db1c_username, password=self.db1c_password)

    def select(self):
        pass

    def insert(self):
        pass

    def update(self):
        pass

    def db1c_sync(self):
        try:
            if self.db1c_conn is None or self.conn.closed:
                self.connect_db1c()
            if self.conn is None or self.conn.closed:
                self.connect_cars()
                local_conn = True
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
                with self.db1c_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as source_cur:
                    query = sql.SQL('SELECT * FROM {db1c_table_name}').format(
                        db1c_table_name=sql.Identifier(self.db1c_table_name))
                    source_cur.execute(query)
                    rows = source_cur.fetchall()
                if self.table_name.startswith('_reference'):
                    updates = [sql.SQL('{column_name} = excluded.{column_name}'
                                       ).format(column_name=sql.Identifier(i[0])) for i in self.columns]
                    conditions = sql.SQL('r._version < excluded._version')
                    query = sql.SQL('insert into {table_name} as r values ({placeholders}) on conflict ({p_key}) do update set {'
                                    'updates} where {conditions}'
                                    ).format(table_name=sql.Identifier(self.table_name)
                                             , placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in self.columns)
                                             , p_key=sql.SQL(', ').join(sql.Identifier(_) for _ in self.primary)
                                             , updates=sql.SQL(', ').join(updates), conditions=conditions)
                elif self.table_name.startswith('_inforg'):
                    query = sql.SQL('insert into {table_name} as r values ({placeholders}) on conflict do nothing'
                                    ).format(table_name=sql.Identifier(self.table_name)
                                             , placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in self.columns))
                for row in rows:
                    target_cur.execute(query, row)
                    # print(target_cur.mogrify(query, row))
        finally:
            self.db1c_conn.close()
            self.conn.commit()
            if local_conn:
                self.conn.close()


if __name__ == '__main__':
    pass
