from psycopg2 import sql
from database import sql_handler


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
        return [cl(**dict(row)) for row in _obj.dql_handler(select_clause)[0]]
