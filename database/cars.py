import psycopg2
from psycopg2 import sql, extras, errors
import json
import re
from abc import ABC


class DataBase:
    CREDENTIALS_FILE = r'database/sql_credentials.json'

    def __init__(self, db):
        self.__connection = None
        self.__cursor = None
        self.db = db

    @property
    def connection(self):
        if self.__connection is None or self.__connection.closed:
            self.__connection = self.open_connection()
        return self.__connection

    @property
    def cursor(self):
        if self.__cursor is None or self.__cursor.closed:
            self.__cursor = self.connection.cursor(cursor_factory=extras.DictCursor)
        return self.__cursor

    def open_connection(self):
        credentials = self.load_credentials()
        return psycopg2.connect(host=credentials[self.db]['host'], port=credentials[self.db]['port'],
                                database=credentials[self.db]['database'], user=credentials[self.db]['user'],
                                password=credentials[self.db]['password'])

    def load_credentials(self):
        with open(self.CREDENTIALS_FILE, 'r') as f:
            return json.load(f)

    def disconnect(self):
        if not self.__cursor.closed:
            self.__cursor.close()
        if not self.__connection.closed:
            self.__connection.commit()
            self.__connection.close()

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.__connection.rollback()
            self.__connection.close()
            print(exc_val)


class Table(ABC):
    def __init__(self, db: DataBase, table_name):
        self.db = db
        self.table_name = table_name
        self.__columns = None
        self.__primary_key = None
        self.__data = None

    def dql_handler(self, *queries):
        result = []
        with self.db as cur:
            for query in queries:
                cur.execute(query)
                result.append(cur.fetchall())
        return result

    def dml_handler(self, *queries):
        result = []
        try:
            with self.db as cur:
                for query in queries:
                    cur.execute(query)
                    result.append({'lastrowid': cur.fetchone(), 'rowcount': cur.rowcount})
        except errors.UniqueViolation as e:
            return str(e)
        except errors.ForeignKeyViolation as e:
            return str(e)
        return result

    @property
    def columns(self):
        if self.__columns is None:
            query = sql.SQL('SELECT column_name FROM information_schema.columns where table_name = {'
                            'table_name}').format(table_name=sql.Literal(self.table_name))
            self.__columns = self.dql_handler(query)[0]
        return self.__columns

    @property
    def primary_key(self):
        if self.__primary_key is None:
            query = sql.SQL('SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid '
                            'AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = {table_name}::regclass '
                            'and i.indisprimary is true').format(table_name=sql.Literal(self.table_name))
            self.__primary_key = self.dql_handler(query)[0][0]
        return self.__primary_key

    @property
    def data(self):
        if self.__data is None:
            query = sql.SQL('SELECT * FROM {table_name}').format(table_name=sql.Identifier(self.table_name))
            self.__data = self.dql_handler(query)[0]
        return self.__data


class Db1cTable(Table):
    def __init__(self, table_name):
        super().__init__(DataBase('db1c'), table_name)


class CarsTable(Table):

    def __init__(self, table_name):
        super().__init__(DataBase('cars'), table_name)

    def sync(self):
        db1c_table = Db1cTable(self.table_name)
        _reference_pattern = re.compile(r'^_(reference|document)\d+$')
        with self.db as cur:
            for row in db1c_table.data:
                if re.fullmatch(_reference_pattern, self.table_name):
                    updates = [sql.SQL('{column_name} = excluded.{column_name}'
                                       ).format(column_name=sql.Identifier(i[0])) for i in db1c_table.columns]
                    conditions = sql.SQL('r._version < excluded._version')
                    query = sql.SQL(
                        'insert into {table_name} as r values ({placeholders}) on conflict ({p_key}) do update set {'
                        'updates} where {conditions}'
                    ).format(table_name=sql.Identifier(self.table_name),
                             placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in db1c_table.columns),
                             p_key=sql.SQL(', ').join(sql.Identifier(_) for _ in self.primary_key),
                             updates=sql.SQL(', ').join(updates), conditions=conditions)
                else:
                    query = sql.SQL('insert into {table_name} as r values ({placeholders}) on conflict do nothing'
                                    ).format(table_name=sql.Identifier(self.table_name),
                                             placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in self.columns))
                cur.execute(query, row)
            self.db.disconnect()

    def get_data(self, where=None):
        where_clause = where if where else sql.SQL('')
        query = sql.SQL('SELECT * FROM {view} {where}').format(view=sql.Identifier(self.table_name),
                                                               where=where_clause)
        return self.dql_handler(query)[0]

    def insert_data(self, columns_data):
        insert = (sql.SQL('insert into {table_name} (').format(table_name=sql.Identifier(self.table_name)) +
                  sql.SQL(', ').join(sql.Identifier(col) for col in columns_data.keys()) +
                  sql.SQL(') values (') +
                  sql.SQL(', ').join(sql.Literal(val) for val in columns_data.values()) +
                  sql.SQL(') returning id')
                  )
        return self.dml_handler(insert)

    def update_data(self, columns_data, condition_data):
        update = (sql.SQL('update {table_name} set ').format(table_name=sql.Identifier(self.table_name)) +
                  sql.SQL(', ').join(sql.SQL('{column_name}={value}').format(
                      column_name=sql.Identifier(k), value=sql.Literal(v)) for k, v in columns_data.items()) +
                  sql.SQL(' WHERE ') +
                  sql.SQL(' and ').join(sql.SQL('{column_name}={value}').format(
                      column_name=sql.Identifier(k), value=sql.Literal(v)) for k, v in condition_data.items()) +
                  sql.SQL(' returning id')
                  )
        return bool(self.dml_handler(update)[0]['rowcount'])

    def delete_data(self, condition_data):
        delete = (sql.SQL('delete from {table_name}').format(table_name=sql.Identifier(self.table_name)) +
                  sql.SQL(' WHERE ') +
                  sql.SQL(' and ').join(sql.SQL('{column_name}={value}').format(
                      column_name=sql.Identifier(k), value=sql.Literal(v)) for k, v in condition_data.items()) +
                  sql.SQL(' returning id')
                  )
        return bool(self.dml_handler(delete)[0]['rowcount'])


if __name__ == '__main__':
    pass
