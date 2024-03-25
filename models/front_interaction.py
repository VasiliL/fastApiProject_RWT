from pydantic import BaseModel
from datetime import datetime, date
from psycopg2 import sql
from typing import Optional, List
from decimal import Decimal


class MyModel(BaseModel):
    @classmethod
    def generate_select_query(cls):
        column_names = list(cls.__annotations__.keys())
        return sql.SQL("SELECT {}").format(
            sql.SQL(",").join(map(sql.Identifier, column_names))
        )


class Car(MyModel, BaseModel):
    id: int
    description: str
    plate_number: Optional[str]
    owner: Optional[str]
    vin: Optional[str]
    year: Optional[float]
    engine_hp: Optional[float]
    weight_capacity: Optional[float]
    volume: Optional[float]
    weight_own: Optional[float]
    car_type: Optional[str]
    car_model: Optional[str]
    trailer_description: Optional[str]


class Person(MyModel, BaseModel):
    id: int
    fio: str
    company: Optional[str]
    position: Optional[str]
    driver_license: Optional[str] = None


class Invoice(MyModel, BaseModel):
    id: int
    client: Optional[str] = None
    route: Optional[str] = None
    cargo: Optional[str] = None
    weight: Decimal
    price: Decimal
    departure_date: datetime
    arrival_date: datetime
    shipper: Optional[str] = None
    consignee: Optional[str] = None
    arrival_point: Optional[str] = None
    departure_point: Optional[str] = None
    client_contract: Optional[str] = None


class DriverPlace(MyModel, BaseModel):
    id: Optional[int] = None
    date_place: date
    car_id: int
    driver_id: int
    plate_number: Optional[str] = None
    fio: Optional[str] = None


class Run(MyModel, BaseModel):
    id: Optional[int] = None
    car_id: List[int] | int
    driver_id: Optional[int] = None
    invoice_id: Optional[int] = None
    date_departure: Optional[date] = None
    invoice_document: Optional[str] = None
    waybill: Optional[str] = None
    weight: Optional[Decimal] = Decimal(0)
    weight_arrival: Optional[Decimal] = Decimal(0)
    client_weight: Optional[Decimal] = Decimal(0)
    client_weight_arrival: Optional[Decimal] = Decimal(0)
    date_arrival: Optional[date] = None
    reg_number: Optional[str] = None
    reg_date: Optional[date] = None
    acc_number: Optional[str] = None
    acc_date: Optional[date] = None
    client: Optional[str] = None
    route: Optional[str] = None
    cargo: Optional[str] = None


class RunUpdaterClientWeight(MyModel, BaseModel):
    id: int
    client_weight: Optional[Decimal] = Decimal(0)
    client_weight_arrival: Optional[Decimal] = Decimal(0)


class RunUpdaterWeight(MyModel, BaseModel):
    id: int
    weight: Optional[Decimal] = Decimal(0)
    weight_arrival: Optional[Decimal] = Decimal(0)
    date_departure: Optional[date] = None
    date_arrival: Optional[date] = None
