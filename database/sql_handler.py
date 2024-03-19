import psycopg2
from psycopg2 import sql, extras, errors
import json
import re
from abc import ABC


class DataBase:
    CREDENTIALS_FILE = r"database/sql_credentials.json"

    def __init__(self, db):
        self.db = db

    def open_connection(self):
        credentials = self.load_credentials()
        return psycopg2.connect(
            host=credentials[self.db]["host"],
            port=credentials[self.db]["port"],
            database=credentials[self.db]["database"],
            user=credentials[self.db]["user"],
            password=credentials[self.db]["password"],
        )

    @staticmethod
    def open_cursor(conn):
        return conn.cursor(cursor_factory=extras.DictCursor)

    def load_credentials(self):
        with open(self.CREDENTIALS_FILE, "r") as f:
            return json.load(f)


class Table(ABC):
    def __init__(self, db: DataBase, table_name):
        self.db = db
        self.table_name = table_name
        self.__columns = None
        self.__primary_key = None
        self.__data = None
        self.table_conn = None
        self.table_cur = None

    def dql_handler(self, *queries):
        result = []
        for query in queries:
            self.table_cur.execute(query)
            _result = self.table_cur.fetchall()
            if _result:
                result.append(_result)
        return result if result else None

    def dml_handler(self, *queries):
        result = []
        try:
            for query in queries:
                self.table_cur.execute(query)
                result.append(
                    {
                        "lastrowid": self.table_cur.fetchone(),
                        "rowcount": self.table_cur.rowcount,
                    }
                )
                self.table_conn.commit()
        except errors.UniqueViolation:
            self.table_conn.rollback()
            return str("duplicate keys forbidden")
        except errors.ForeignKeyViolation:
            self.table_conn.rollback()
            return str("car or driver or invoice does not exists or defined")
        return result

    @staticmethod
    def create_where_statement(conditions: dict) -> sql.Composed:
        return sql.SQL(" WHERE ") + sql.SQL(" and ").join(sql.SQL("{column_name}={value}").format(
            column_name=sql.Identifier(k), value=sql.Literal(v)) for k, v in conditions.items())

    @property
    def columns(self):
        if self.__columns is None:
            query = sql.SQL(
                "SELECT column_name FROM information_schema.columns where table_name = {"
                "table_name}"
            ).format(table_name=sql.Literal(self.table_name))
            sql_response = self.dql_handler(query)
            self.__columns = sql_response[0] if sql_response else None
        return self.__columns

    @property
    def primary_key(self):
        if self.__primary_key is None:
            query = sql.SQL(
                "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                "AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = {table_name}::regclass "
                "and i.indisprimary is true"
            ).format(table_name=sql.Literal(self.table_name))
            sql_response = self.dql_handler(query)
            self.__primary_key = sql_response[0][0] if sql_response else None
        return self.__primary_key

    @property
    def data(self):
        if self.__data is None:
            query = sql.SQL("SELECT * FROM {table_name}").format(
                table_name=sql.Identifier(self.table_name)
            )
            sql_response = self.dql_handler(query)
            self.__data = sql_response[0] if sql_response else None
        return self.__data

    def __enter__(self):
        self.table_conn = self.db.open_connection()
        self.table_cur = self.db.open_cursor(self.table_conn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.table_conn.commit()
            self.table_cur.close()
            self.table_conn.close()
        else:
            self.table_conn.rollback()
            self.table_cur.close()
            self.table_conn.close()
            print(exc_val)


class Db1cTable(Table):
    def __init__(self, table_name):
        super().__init__(DataBase("db1c"), table_name)


class CarsTable(Table):
    def __init__(self, table_name):
        super().__init__(DataBase("cars"), table_name)

    def create_temp_table(self):
        query = sql.SQL(
            "CREATE TEMP TABLE {name} (like {table_name} excluding all including INDEXES) on commit drop; "
            "alter table {name} drop IF EXISTS id;"
        ).format(name=sql.Identifier('temp_' + self.table_name),
                 table_name=sql.Identifier(self.table_name))
        self.table_cur.execute(query)
        return f'temp_{self.table_name}'

    def fill_temp_table(self):
        db1c_table = Db1cTable(self.table_name)
        temp_table = self.create_temp_table()
        with db1c_table:
            query = sql.SQL(
                "insert into {table_name} values ({placeholders})"
            ).format(
                table_name=sql.Identifier(temp_table),
                placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in db1c_table.columns)
            )
            if db1c_table.data:
                for row in db1c_table.data:
                    self.table_cur.execute(query, row)
        return temp_table

    def sync(self):
        _reference_pattern = re.compile(r"^_(reference|document)\d+$")
        temp_table = self.fill_temp_table()
        columns = list(self.columns)
        if ['id'] in columns:
            columns.remove(['id'])
            # the query for delete all records from the main table that are not in the temporary table
            delete_query = sql.SQL("delete from {table_name} where {p_key} not in (select {p_key} from {temp_table})"
                                   ).format(
                table_name=sql.Identifier(self.table_name),
                p_key=sql.Identifier(self.primary_key[0]),
                temp_table=sql.Identifier(temp_table)
            )
            self.table_cur.execute(delete_query)
        # the query for insert all records from the temporary table that are not in the main table
            updates = [sql.SQL("{column_name} = excluded.{column_name}").format(column_name=sql.Identifier(i[0]))
                       for i in columns
                       ]
            conditions = sql.SQL("r._version < excluded._version")
            insert_query = sql.SQL(
                "insert into {table_name} as r ({column_names}) select {column_names} from {temp_table} "
                "on conflict ({p_key}) do "
                "update set {updates} where {conditions}"
            ).format(
                table_name=sql.Identifier(self.table_name),
                temp_table=sql.Identifier(temp_table),
                p_key=sql.SQL(", ").join(sql.Identifier(_) for _ in self.primary_key),
                updates=sql.SQL(", ").join(updates),
                column_names=sql.SQL(", ").join([sql.Identifier(i[0]) for i in columns]),
                conditions=conditions,
            )
        elif not bool(self.primary_key) and not re.fullmatch(_reference_pattern, self.table_name):
            insert_query = sql.SQL(
                "truncate {table_name};"
                "insert into {table_name} as r ({column_names}) select {column_names} from {temp_table}"
            ).format(
                table_name=sql.Identifier(self.table_name),
                temp_table=sql.Identifier(temp_table),
                column_names=sql.SQL(", ").join([sql.Identifier(i[0]) for i in columns]),
            )
        else:
            insert_query = sql.SQL('select 1;')
        self.table_cur.execute(insert_query)

    def get_data(self, where=None):
        where_clause = where if where else sql.SQL("")
        query = sql.SQL("SELECT * FROM {view} {where}").format(
            view=sql.Identifier(self.table_name), where=where_clause
        )
        sql_response = self.dql_handler(query)
        return sql_response[0] if sql_response else None

    def insert_data(self, columns_data):
        insert = (
                sql.SQL("insert into {table_name} (").format(
                    table_name=sql.Identifier(self.table_name)
                )
                + sql.SQL(", ").join(sql.Identifier(col) for col in columns_data.keys())
                + sql.SQL(") values (")
                + sql.SQL(", ").join(sql.Literal(val) for val in columns_data.values())
                + sql.SQL(") returning id")
        )
        return self.dml_handler(insert)

    def update_data(self, columns_data, condition_data):
        select = (
                sql.SQL("select {columns} from {table_name}").format(
                    columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns_data.keys()),
                    table_name=sql.Identifier(self.table_name),)
                + self.create_where_statement(condition_data)
        )
        try:
            sql_response = self.dql_handler(select)
            result = sql_response[0][0]
            result = {k: result[k] for k in columns_data.keys()}
            if result == columns_data:
                return False
        except IndexError:
            return str("The record with the ID does not exist or ID is not defined.")
        update = (
                sql.SQL("update {table_name} set ").format(table_name=sql.Identifier(self.table_name))
                + sql.SQL(", ").join(sql.SQL("{column_name}={value}").format(column_name=sql.Identifier(k),
                                                                             value=sql.Literal(v))
                                     for k, v in columns_data.items())
                + self.create_where_statement(condition_data)
                + sql.SQL(" returning id")
        )
        return self.dml_handler(update)

    def delete_data(self, condition_data):
        delete = (
                sql.SQL("delete from {table_name}").format(
                    table_name=sql.Identifier(self.table_name)
                )
                + self.create_where_statement(condition_data)
                + sql.SQL(" returning id")
        )
        return self.dml_handler(delete)
