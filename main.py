from datetime import datetime, date
import pytz

from fastapi import FastAPI
from my_psql import cars
import json
from models.ui_inputs import DriverSet, CarSet, WaybillSet, TnSet, Invoice
from psycopg2 import sql
from typing import List

app = FastAPI()

TABLES = {'persons': ('_reference300', '_reference155', '_reference89',),
          'cars': ('_reference262', '_reference211', '_reference259'),
          'cargo': ('_reference124',),
          'routes': ('_reference207', '_reference207_vt7419', '_reference225',),
          'counterparty': ('_reference111', '_reference110',),
          'shipment_requests': ('_document350', '_document350_vt1855', '_document365_vt2454')}
STATIC_VIEWS = {'persons', 'cars', 'cargo', 'routes', 'counterparty', 'invoices', 'react_drivers', 'react_cars',
                'runs_view', }


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.patch('/bi/update_data/{data}')
def update_data(data):
    if data in TABLES:
        for table in TABLES[data]:
            _obj = cars.BItable(table)
            _obj.db1c_sync()
        return {"message": f"{data} updated"}
    else:
        return {"message": f"Can not update {data}"}


@app.get('/bi/get/{data}')
def get_data(data):
    """Для получения данных о водителях (react_drivers) и машинах(react_cars), runs_view"""
    if data in STATIC_VIEWS:
        _obj = cars.BIView(data)
        data = [dict(row) for row in _obj.get_data()]
        for row in data:
            for key, value in row.items():
                if isinstance(value, bytes) and len(value) == 16:
                    row[key] = str(uuid.UUID(bytes=value))
        json_data = json.dumps(data, default=str, ensure_ascii=False)
        return json_data
    return {"message": "No data"}


@app.get('/bi/get_condition/drivers_place')
def drivers_place(start_day: date, end_day: date):
    """Расстановка водителей на машины"""
    _obj = cars.BIView('drivers_place')
    where_condition = sql.SQL('WHERE date between {start_date} and {end_date};').format(
        start_date=sql.Literal(start_day.strftime('%Y-%m-%d')), end_date=sql.Literal(end_day.strftime('%Y-%m-%d')))
    json_data = json.dumps([dict(row) for row in _obj.get_data(where_condition)], default=str, ensure_ascii=False)
    return json_data


@app.get('/bi/get_condition/react_invoices', response_model=List[Invoice])
def react_invoices(day: date):
    """Расстановка машин на маршруты: информация про заявки"""
    _obj = cars.BIView('react_invoices')
    where_condition = sql.SQL('WHERE {day} between {departure_date} and {arrival_date};').format(
        departure_date=sql.Identifier('departure_date'), arrival_date=sql.Identifier('arrival_date'),
        day=sql.Literal(day.strftime('%Y-%m-%d')))
    # json_data = json.dumps([dict(row) for row in _obj.get_data(where_condition)], default=str, ensure_ascii=False)
    return [Invoice(**dict(row)) for row in _obj.get_data(where_condition)]


@app.get('/bi/get_condition/cars_place')
def cars_place(day: date):
    """Расстановка машин на маршруты: машины на маршруте"""
    _obj = cars.BIView('runs_view')
    where_condition = sql.SQL('WHERE date(date_departure) = {day};').format(day=sql.Literal(day.strftime('%Y-%m-%d')))
    json_data = json.dumps([dict(row) for row in _obj.get_data(where_condition)], default=str, ensure_ascii=False)
    return json_data


@app.get('/bi/get_condition/runs_view')
def runs_view(start_day: date, end_day: date):
    """Рейсы"""
    _obj = cars.BIView('runs_view')
    where_condition = sql.SQL('WHERE date_departure between {start_date} and {end_date};').format(
        start_date=sql.Literal(start_day.strftime('%Y-%m-%d')), end_date=sql.Literal(end_day.strftime('%Y-%m-%d')))
    json_data = json.dumps([dict(row) for row in _obj.get_data(where_condition)], default=str, ensure_ascii=False)
    return json_data


@app.post('/bi/post/drivers_place')
def set_drivers_place(data: DriverSet):
    pass


@app.post('/bi/post/cars_place')
def set_cars_place(data: CarSet):
    pass


@app.post('/bi/post/set_waybills')
def set_waybills(data: WaybillSet):
    pass


@app.post('/bi/post/set_tn')
def set_tn(data: TnSet):
    pass
