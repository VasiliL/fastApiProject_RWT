from datetime import date
from fastapi import APIRouter
from database import cars
from models.front_interaction import Car, Person, Invoice, DriverPlace, Run
from psycopg2 import sql
from typing import List, Optional
import routes.func as func

router = APIRouter()

TABLES = {'persons': ('_reference300', '_reference155', '_reference89',),
          'cars': ('_reference262', '_reference211', '_reference259'),
          'invoices': ('_document350', '_document350_vt1855', '_document365_vt2454', '_reference124', '_reference207',
                       '_reference207_vt7419', '_reference225', '_reference111', '_reference110',)}
STATIC_VIEWS = {'persons', 'cars', 'cargo', 'routes', 'counterparty', 'invoices', 'react_drivers', 'react_cars',
                'runs_view', }


@router.get("/")
async def root():
    """
    Root
    This method is responsible for handling requests to the root endpoint ("/") in the application.
    Returns:
        A dictionary containing the message "Hello World".
    """
    return {"message": "Hello World"}


@router.patch('/api/update_data/{data}')
def update_data(data):
    """
    Updates the data for a given table.
    Parameters:
    - data (str): The name of the table to update Can be: 'persons', 'cars', 'invoices'.
    Returns:
    - dict: A dictionary with a message indicating the status of the update.
    """
    if data in TABLES:
        for table in TABLES[data]:
            _obj = cars.CarsTable(table)
            with _obj:
                _obj.sync()
        return {"message": f"{data} updated"}
    else:
        return {"message": f"Can not update {data}"}


@router.get('/api/cars', response_model=List[Car])
async def get_cars():
    """
    Get a list of cars from the API.
    Returns a list of Car objects.
    Args: None
    Returns: A list of Car objects.
    Example:
        cars = get_cars()
    """
    view = 'cars'
    cl = Car
    where = sql.SQL('''where car_type in ('Грузовые автомобили тягачи седельные', 'Тягачи седельные 6х4',
     'Грузовые самосвалы 8х4')''')
    return await func.get_view_data(view, cl, where)


@router.get('/api/drivers', response_model=List[Person])
async def get_drivers():
    """
    Function: get_drivers
    Description:
    This function is an API endpoint that retrieves a list of drivers from the 'persons' view in the database.
    It uses the 'Person' model to define the structure of the data.
    Parameters: None
    Returns: A list of 'Person' objects.
    Example Usage: GET /api/drivers
    """
    view = 'persons'
    cl = Person
    where = sql.SQL('where position = {}').format(sql.Literal('Водитель-экспедитор'))
    return await func.get_view_data(view, cl, where)


@router.get('/api/invoices', response_model=List[Invoice])
async def get_invoices(day: Optional[date] = None):
    """
    Get invoices based on the given day.
    Parameters:
    - day: Optional[date] (default=None)
        The specific day to filter the invoices. If not provided, it will use today's date.
    Returns:
    - List[Invoice]
        A list of invoice objects.
    """
    view = 'invoices'
    cl = Invoice
    day = day or date.today()
    where = sql.SQL(' WHERE {day} between {departure_date} and {arrival_date}').format(
        departure_date=sql.Identifier('departure_date'), arrival_date=sql.Identifier('arrival_date'),
        day=sql.Literal(day.strftime('%Y-%m-%d'))) if day else None
    return await func.get_view_data(view, cl, where)


@router.get('/api/drivers_place', response_model=List[DriverPlace])
async def get_drivers_place(start_day: date, end_day: date):
    """
    get_drivers_place
    Method signature:
        async def get_drivers_place(start_day: date, end_day: date) -> List[DriverPlace]
    Description:
        This method retrieves the driver's place data from the API.
    Parameters:
        start_day (date): The start date for the data retrieval.
        end_day (date): The end date for the data retrieval.
    Returns:
        List[DriverPlace]: A list of DriverPlace objects representing the driver's place data.
    """
    view = 'drivers_place'
    cl = DriverPlace
    where = sql.SQL('WHERE date between {start_date} and {end_date}').format(
        start_date=sql.Literal(start_day.strftime('%Y-%m-%d')), end_date=sql.Literal(end_day.strftime('%Y-%m-%d')))
    return await func.get_view_data(view, cl, where)


@router.post('/api/drivers_place')
def set_drivers_place(data: DriverPlace) -> str | int:
    """
    Endpoint to set the place for drivers.
    Parameters:
        data: DriverPlace - The data object containing the information of the driver's place to be set.
    Returns:
        ID of the inserted data or error message
    """
    columns = ('_date', 'driver', 'car')
    columns_data = dict(zip(columns, [data.date, data.driver_id, data.car_id]))
    _obj = cars.CarsTable('drivers_place_table')
    result = _obj.insert_data(columns_data)
    return result if type(result) is str else result[0]['lastrowid'][0]


@router.put('/api/drivers_place')
async def put_drivers_place(data: DriverPlace) -> str | bool:
    """
    Update the drivers_place_table in the API using an HTTP PUT request.
    Parameters:
    - data: DriverPlace object representing the data to be updated in the drivers_place_table.
    Returns:
        True if successful, False otherwise
    """
    columns = ('_date', 'driver', 'car')
    condition_columns = ('id',)
    columns_data = dict(zip(columns, [data.date, data.driver_id, data.car_id]))
    condition_data = dict(zip(condition_columns, [data.id, ]))
    _obj = cars.CarsTable('drivers_place_table')
    result = _obj.update_data(columns_data, condition_data)
    return True if type(result) is list else result


@router.delete('/api/drivers_place')
async def delete_drivers_place(data: int) -> str | bool:
    """
    Delete Drivers Place
    Deletes a drivers place from the database based on the provided data.
    Parameters:
    - data (int): The ID of the drivers place to be deleted.
    Returns: True if successful, False otherwise
    """
    condition_columns = ('id',)
    condition_data = dict(zip(condition_columns, [data, ]))
    _obj = cars.CarsTable('drivers_place_table')
    result = _obj.delete_data(condition_data)
    try:
        return result if type(result) is str else bool(result[0]['rowcount'])
    except TypeError as e:
        return False


@router.get('/api/runs', response_model=List[Run])
async def get_runs(start_day: date, end_day: date):
    """
    Method: get_runs
    Description:
    This method retrieves a list of Run objects within the specified date range.
    Parameters:
    - start_day (date): The start date of the range.
    - end_day (date): The end date of the range.
    Returns: List of runs dicts.
    """
    view = 'runs_view'
    cl = Run
    where = sql.SQL('WHERE date_departure between {start_date} and {end_date}').format(
        start_date=sql.Literal(start_day.strftime('%Y-%m-%d')), end_date=sql.Literal(end_day.strftime('%Y-%m-%d')))
    return await func.get_view_data(view, cl, where)


@router.post('/api/runs')
async def post_runs(data: Run) -> str | int:
    """
    Method: post_runs
    Description:
    This method is an endpoint for posting runs data to the '/api/runs' route. It takes a single parameter 'data'
    of type Run. The method inserts the provided data into the 'runs' table
    Parameters:
    - data (Run): An object containing the run data to be inserted into the database.
    Returns: ID of the inserted data
    """
    columns = ('invoice_document', 'waybill', 'weight', 'date_departure', 'date_arrival', 'car', 'driver', 'invoice')
    columns_data = dict(zip(columns, [data.invoice_document, data.waybill, data.weight, data.date_departure,
                                      data.date_arrival, data.car, data.driver, data.invoice]))
    _obj = cars.CarsTable('runs')
    result = _obj.insert_data(columns_data)
    return result if type(result) is str else result[0]['lastrowid'][0]


@router.put('/api/runs')
async def put_runs(data: Run):
    """
    Method Name: put_runs
    Description:
    This method is used to update runs data in the 'runs' table. It takes a parameter 'data' of type Run, and updates
    the columns of the 'runs' table based on the values provided in the
    * 'data' object.
    Parameters:
    - data: Run
        An object of type Run that contains the new values to be updated in the 'runs' table.
    Returns:
    - result: True if successful, False otherwise
    """
    columns = ('invoice_document', 'waybill', 'weight', 'date_departure', 'date_arrival', 'car', 'driver', 'invoice')
    condition_columns = ('id',)
    columns_data = dict(zip(columns, [data.invoice_document, data.waybill, data.weight, data.date_departure,
                                      data.date_arrival, data.car, data.driver, data.invoice]))
    condition_data = dict(zip(condition_columns, [data.id, ]))
    _obj = cars.CarsTable('runs')
    result = _obj.update_data(columns_data, condition_data)
    return True if type(result) is list else result


@router.delete('/api/runs')
async def delete_runs(data: int):
    """
    Delete Runs
    Deletes runs from the 'runs' table based on the given condition.
    Parameters:
    - data (int): The run id.
    Returns: True if successful, False otherwise
    """
    condition_columns = ('id',)
    condition_data = dict(zip(condition_columns, [data, ]))
    _obj = cars.CarsTable('runs')
    result = _obj.delete_data(condition_data)
    try:
        return result if type(result) is str else bool(result[0]['rowcount'])
    except TypeError as e:
        return False
