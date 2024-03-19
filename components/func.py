from typing import List, Tuple
from pydantic import BaseModel
from psycopg2 import sql
from database import sql_handler
from fastapi.responses import JSONResponse
from fastapi import HTTPException


async def get_query(view, cl, where=None):
    """
    Parameters:
    - view: The name of the database view to retrieve data from. (String)
    - cl: An instance of a class that generates the select query for the view. (Object)
    - where: Optional parameter specifying the where condition for the query. (String)
    Returns:
    - A concatenated string of the select query and the where condition, which can be used to retrieve data from
    the specified view.
    """
    select_clause = cl.generate_select_query() + sql.SQL(" from {table}").format(
        table=sql.Identifier(view)
    )
    where_condition = where if where else sql.SQL("")
    return select_clause + where_condition


async def get_view_data(view, cl, where=None):
    """
    This method, get_view_data, is an asynchronous method that retrieves view data based on the provided parameters.
    It returns a list of objects based on the query results.
    Parameters:
    - view: The view object that represents the table or view from which the data is retrieved.
    - cl: The class that represents the object type to be returned.
    - where: An optional parameter that specifies the conditions for filtering the data.
    Returns:
    - A list of objects (instances of the class specified by the 'cl' parameter) based on the query results.
    """
    _obj = sql_handler.CarsTable(view)
    with _obj:
        select_clause = await get_query(view, cl, where)
        result = _obj.dql_handler(select_clause)
        if result:
            return [cl(**dict(row)) for row in result[0]]


async def post_multiple_objects(data: List[BaseModel], table_name: str):
    result = []
    with sql_handler.CarsTable(table_name) as _obj:
        for item in data:
            _dict = item.dict(exclude_none=True).copy()
            if "id" in _dict:
                del _dict["id"]
            _result = _obj.insert_data(_dict)
            result.append(True if isinstance(_result, list) else _result)
    return JSONResponse(status_code=200, content=result)


async def put_multiple_objects(data: List[BaseModel], table_name: str, conditions: Tuple[str]):
    result = []
    with sql_handler.CarsTable(table_name) as _obj:
        for item in data:
            try:
                data_dict = {k: v for k, v in item.dict(exclude_none=True).items() if k not in conditions}
                condition_dict = {k: v for k, v in item.dict(exclude_none=True).items() if k in conditions}
                _result = _obj.update_data(data_dict, condition_dict)
                result.append(True if isinstance(_result, list) else _result)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Ошибка в данных {condition_dict}: {e}")
    return JSONResponse(status_code=200, content=result)
