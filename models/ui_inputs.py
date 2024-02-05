from pydantic import BaseModel
from datetime import datetime, date
from psycopg2 import sql
from typing import Optional
from decimal import Decimal


class MyModel(BaseModel):
    @classmethod
    def generate_select_query(cls):
        column_names = cls.__annotations__.keys()
        return sql.SQL('SELECT {}').format(sql.SQL(',').join(map(sql.Identifier, column_names)))


class Car(MyModel, BaseModel):
    id: int
    description: str
    plate_number: str
    vin: str
    year: float
    engine_hp: float
    weight_capacity: float
    volume: float
    weight_own: float
    car_type: str


class Person(MyModel, BaseModel):
    id: int
    fio: str
    code: str
    position: str
    inn: str
    snils: str


class Invoice(MyModel, BaseModel):
    id: int
    client: str
    route: str
    cargo: str
    weight: float
    price: float
    departure_date: datetime
    arrival_date: datetime


class DriverPlace(MyModel, BaseModel):
    id: int
    date: date
    car_id: int
    driver_id: int
    plate_number: Optional[str]
    fio: Optional[str]


class Run(MyModel, BaseModel):
    id: int
    car: int
    driver: int
    invoice: int
    date_departure: date
    invoice_document: Optional[str]
    waybill: Optional[str]
    weight: Optional[Decimal]
    date_arrival: Optional[date]
    reg_number: Optional[str]
    reg_date: Optional[date]
    acc_number: Optional[str]
    acc_date: Optional[date]
