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


@app.patch('/bi/update_data/{data}')
def update_data(data):
    """Update data from 1C tables: persons, cars, invoices."""
    if data in TABLES:
        for table in TABLES[data]:
            _obj = cars.BItable(table)
            _obj.db1c_sync()
        return {"message": f"{data} updated"}
    else:
        return {"message": f"Can not update {data}"}


@app.get('/bi/cars', response_model=List[Car])
async def get_cars():
    """Returns all cars"""
    view = 'cars'
    cl = Car
    where = None
    return await get_view_data(view, cl, where)


@app.get('/bi/drivers', response_model=List[Person])
async def get_drivers():
    """Returns all drivers"""
    view = 'persons'
    cl = Person
    where = sql.SQL('where position = {}').format(sql.Literal('Водитель-экспедитор'))
    return await get_view_data(view, cl, where)


@app.get('/bi/invoices', response_model=List[Invoice])
async def get_invoices(day: Optional[date] = None):
    """Returns all invoices actual for a given day  or for today"""
    view = 'invoices'
    cl = Invoice
    day = day or date.today()
    where = sql.SQL(' WHERE {day} between {departure_date} and {arrival_date}').format(
        departure_date=sql.Identifier('departure_date'), arrival_date=sql.Identifier('arrival_date'),
        day=sql.Literal(day.strftime('%Y-%m-%d'))) if day else None
    return await get_view_data(view, cl, where)


@app.get('/bi/drivers_place', response_model=List[DriverPlace])
async def get_drivers_place(start_day: date, end_day: date):
    """Returns drivers on cars places actual for a given range of days"""
    view = 'drivers_place'
    cl = DriverPlace
    where = sql.SQL('WHERE date between {start_date} and {end_date}').format(
        start_date=sql.Literal(start_day.strftime('%Y-%m-%d')), end_date=sql.Literal(end_day.strftime('%Y-%m-%d')))
    return await get_view_data(view, cl, where)


@app.get('/bi/runs', response_model=List[Run])
async def get_runs(start_day: date, end_day: date):
    """Returns drivers on cars places actual for a given range of days"""
    view = 'runs_view'
    cl = Run
    where = sql.SQL('WHERE date_departure between {start_date} and {end_date}').format(
        start_date=sql.Literal(start_day.strftime('%Y-%m-%d')), end_date=sql.Literal(end_day.strftime('%Y-%m-%d')))
    return await get_view_data(view, cl, where)


@app.post('/bi/drivers_place')
def set_drivers_place(data: List[DriverPlace]):
    """Sets drivers on cars places"""
    columns = ('_date', 'driver', 'car')
    _obj = cars.BItable('drivers_place_table')
    for row in data:
        _obj.insert_data(columns, [row.date, row.driver_id, row.car_id])

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
