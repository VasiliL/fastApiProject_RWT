from datetime import datetime, date
import pytz

from fastapi import FastAPI
from database import cars
import json
from models.ui_inputs import Car, Person, Invoice, DriverPlace, Run
from psycopg2 import sql
from typing import List, Optional

app = FastAPI()

TABLES = {'persons': ('_reference300', '_reference155', '_reference89',),
          'cars': ('_reference262', '_reference211', '_reference259'),
          'invoices': ('_document350', '_document350_vt1855', '_document365_vt2454', '_reference124', '_reference207',
                       '_reference207_vt7419', '_reference225', '_reference111', '_reference110',)}
STATIC_VIEWS = {'persons', 'cars', 'cargo', 'routes', 'counterparty', 'invoices', 'react_drivers', 'react_cars',
                'runs_view', }


async def get_query(view, cl, where=None):
    select_clause = cl.generate_select_query() + sql.SQL(' from {table}').format(
        table=sql.Identifier(view))
    where_condition = where if where else sql.SQL('')
    return select_clause + where_condition


async def get_view_data(view, cl, where=None):
    _obj = cars.BIView(view)
    select_clause = await get_query(view, cl, where)
    return [cl(**dict(row)) for row in _obj.custom_select(select_clause)]


@app.get("/")
async def root():
    """Testing 'Hello World' function"""
    return {"message": "Hello World"}


@app.patch('/api/update_data/{data}')
def update_data(data):
    """Update data from 1C tables: persons, cars, invoices."""
    if data in TABLES:
        for table in TABLES[data]:
            _obj = cars.BItable(table)
            _obj.db1c_sync()
        return {"message": f"{data} updated"}
    else:
        return {"message": f"Can not update {data}"}


@app.get('/api/cars', response_model=List[Car])
async def get_cars():
    """Returns all cars"""
    view = 'cars'
    cl = Car
    where = None
    return await get_view_data(view, cl, where)


@app.get('/api/drivers', response_model=List[Person])
async def get_drivers():
    """Returns all drivers"""
    view = 'persons'
    cl = Person
    where = sql.SQL('where position = {}').format(sql.Literal('Водитель-экспедитор'))
    return await get_view_data(view, cl, where)


@app.get('/api/invoices', response_model=List[Invoice])
async def get_invoices(day: Optional[date] = None):
    """Returns all invoices actual for a given day  or for today"""
    view = 'invoices'
    cl = Invoice
    day = day or date.today()
    where = sql.SQL(' WHERE {day} between {departure_date} and {arrival_date}').format(
        departure_date=sql.Identifier('departure_date'), arrival_date=sql.Identifier('arrival_date'),
        day=sql.Literal(day.strftime('%Y-%m-%d'))) if day else None
    return await get_view_data(view, cl, where)


@app.get('/api/drivers_place', response_model=List[DriverPlace])
async def get_drivers_place(start_day: date, end_day: date):
    """Returns drivers on cars places actual for a given range of days"""
    view = 'drivers_place'
    cl = DriverPlace
    where = sql.SQL('WHERE date between {start_date} and {end_date}').format(
        start_date=sql.Literal(start_day.strftime('%Y-%m-%d')), end_date=sql.Literal(end_day.strftime('%Y-%m-%d')))
    return await get_view_data(view, cl, where)


@app.post('/api/drivers_place')
def set_drivers_place(data: DriverPlace):
    """Create drivers on cars places"""
    columns = ('_date', 'driver', 'car')
    _obj = cars.BItable('drivers_place_table')
    _obj.insert_data(columns, [data.date, data.driver_id, data.car_id])
    return {"message": f"{data} created"}


@app.put('/api/drivers_place')
async def put_drivers_place(data: DriverPlace):
    """Change drivers on cars places"""
    columns = ('_date', 'driver', 'car')
    condition_columns = ('id',)
    columns_data = dict(zip(columns, [data.date, data.driver_id, data.car_id]))
    condition_data = dict(zip(condition_columns, [data.id, ]))
    _obj = cars.BItable('drivers_place_table')
    _obj.update_data(columns_data, condition_data)
    return {"message": f"{data} updated"}


@app.delete('/api/drivers_place')
async def delete_drivers_place(data: int):
    """Delete drivers on cars places"""
    condition_columns = ('id',)
    condition_data = dict(zip(condition_columns, [data, ]))
    _obj = cars.BItable('drivers_place_table')
    _obj.delete_data(condition_data)
    return {"message": f"{data} deleted"}


@app.get('/api/runs', response_model=List[Run])
async def get_runs(start_day: date, end_day: date):
    """Returns drivers on cars places actual for a given range of days"""
    view = 'runs_view'
    cl = Run
    where = sql.SQL('WHERE date_departure between {start_date} and {end_date}').format(
        start_date=sql.Literal(start_day.strftime('%Y-%m-%d')), end_date=sql.Literal(end_day.strftime('%Y-%m-%d')))
    return await get_view_data(view, cl, where)


@app.post('/api/runs')
async def post_runs(data: Run):
    """Create runs"""
    columns = ('invoice_document', 'waybill', 'weight', 'date_departure', 'date_arrival', 'car', 'driver', 'invoice')
    _obj = cars.BItable('runs')
    _obj.insert_data(columns, [data.invoice_document, data.waybill, data.weight, data.date_departure,
                               data.date_arrival, data.car, data.driver, data.invoice])
    return {"message": f"{data} created"}


@app.put('/api/runs')
async def put_runs(data: Run):
    """Update runs"""
    columns = ('invoice_document', 'waybill', 'weight', 'date_departure', 'date_arrival', 'car', 'driver', 'invoice')
    condition_columns = ('id',)
    columns_data = dict(zip(columns, [data.invoice_document, data.waybill, data.weight, data.date_departure,
                                      data.date_arrival, data.car, data.driver, data.invoice]))
    condition_data = dict(zip(condition_columns, [data.id, ]))
    _obj = cars.BItable('runs')
    _obj.update_data(columns_data, condition_data)
    return {"message": f"{data} updated"}


@app.delete('/api/runs')
async def delete_runs(data: int):
    """Delete runs"""
    condition_columns = ('id',)
    condition_data = dict(zip(condition_columns, [data, ]))
    _obj = cars.BItable('runs')
    _obj.delete_data(condition_data)
    return {"message": f"{data} deleted"}

#
#
# @app.post('/bi/post/cars_place')
# def set_cars_place(data: CarSet):
#     pass
#
#
# @app.post('/bi/post/set_waybills')
# def set_waybills(data: WaybillSet):
#     pass
#
#
# @app.post('/bi/post/set_tn')
# def set_tn(data: TnSet):
#     pass
