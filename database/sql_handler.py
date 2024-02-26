from typing import List

import numpy as np
import pandas as pd
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
            result.append(self.table_cur.fetchall())
        return result

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
            self.__columns = self.dql_handler(query)[0]
        return self.__columns

    @property
    def primary_key(self):
        if self.__primary_key is None:
            query = sql.SQL(
                "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                "AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = {table_name}::regclass "
                "and i.indisprimary is true"
            ).format(table_name=sql.Literal(self.table_name))
            self.__primary_key = self.dql_handler(query)[0][0]
        return self.__primary_key

    @property
    def data(self):
        if self.__data is None:
            query = sql.SQL("SELECT * FROM {table_name}").format(
                table_name=sql.Identifier(self.table_name)
            )
            self.__data = self.dql_handler(query)[0]
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

    def sync(self):
        db1c_table = Db1cTable(self.table_name)
        with db1c_table:
            _reference_pattern = re.compile(r"^_(reference|document)\d+$")
            for row in db1c_table.data:
                if re.fullmatch(_reference_pattern, self.table_name):
                    updates = [
                        sql.SQL("{column_name} = excluded.{column_name}").format(
                            column_name=sql.Identifier(i[0])
                        )
                        for i in db1c_table.columns
                    ]
                    conditions = sql.SQL("r._version < excluded._version")
                    query = sql.SQL(
                        "insert into {table_name} as r values ({placeholders}) on conflict ({p_key}) do update set {"
                        "updates} where {conditions}"
                    ).format(
                        table_name=sql.Identifier(self.table_name),
                        placeholders=sql.SQL(", ").join(
                            sql.Placeholder() for _ in db1c_table.columns
                        ),
                        p_key=sql.SQL(", ").join(
                            sql.Identifier(_) for _ in self.primary_key
                        ),
                        updates=sql.SQL(", ").join(updates),
                        conditions=conditions,
                    )
                else:
                    query = sql.SQL(
                        "insert into {table_name} as r values ({placeholders}) on conflict do nothing"
                    ).format(
                        table_name=sql.Identifier(self.table_name),
                        placeholders=sql.SQL(", ").join(
                            sql.Placeholder() for _ in self.columns
                        ),
                    )
                self.table_cur.execute(query, row)

    def get_data(self, where=None):
        where_clause = where if where else sql.SQL("")
        query = sql.SQL("SELECT * FROM {view} {where}").format(
            view=sql.Identifier(self.table_name), where=where_clause
        )
        return self.dql_handler(query)[0]

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

    def insert_multiple_data(self, df: pd.DataFrame):
        result = []
        for index, row in df.iterrows():
            _result = self.insert_data(row.to_dict())
            if isinstance(_result, list):
                result += _result
            else:
                result.append(_result)
        return result

    def update_data(self, columns_data, condition_data):
        select = (
            sql.SQL("select {columns} from {table_name}").format(
                columns=sql.SQL(", ").join(
                    sql.Identifier(col) for col in columns_data.keys()
                ),
                table_name=sql.Identifier(self.table_name),
            )
            + self.create_where_statement(condition_data)
        )
        try:
            result = self.dql_handler(select)[0][0]
            result = {k: result[k] for k in columns_data.keys()}
            if result == columns_data:
                return False
        except IndexError:
            return str("The record with the ID does not exist or ID is not defined.")
        update = (
            sql.SQL("update {table_name} set ").format(
                table_name=sql.Identifier(self.table_name)
            )
            + sql.SQL(", ").join(
                sql.SQL("{column_name}={value}").format(
                    column_name=sql.Identifier(k), value=sql.Literal(v)
                )
                for k, v in columns_data.items()
            )
            + self.create_where_statement(condition_data)
            + sql.SQL(" returning id")
        )
        return self.dml_handler(update)

    def update_multiple_data(self, df: pd.DataFrame, condition_data: List[str]):
        result = []
        for index, row in df.iterrows():
            row = row.replace("nan", pd.NA).dropna()
            condition_dict = row[condition_data].to_dict()
            data_dict = row.drop(row[condition_data].index).to_dict()
            _result = self.update_data(data_dict, condition_dict)
            if isinstance(_result, list):
                result += _result
            else:
                result.append(_result)
        return result

    def delete_data(self, condition_data):
        delete = (
            sql.SQL("delete from {table_name}").format(
                table_name=sql.Identifier(self.table_name)
            )
            + self.create_where_statement(condition_data)
            + sql.SQL(" returning id")
        )
        return self.dml_handler(delete)
