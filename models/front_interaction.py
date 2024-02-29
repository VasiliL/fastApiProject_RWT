from pydantic import BaseModel
from datetime import datetime, date
from psycopg2 import sql
from typing import Optional
from decimal import Decimal


class MyModel(BaseModel):
    @classmethod
    def generate_select_query(cls):
        column_names = cls.__annotations__.keys()
        return sql.SQL("SELECT {}").format(
            sql.SQL(",").join(map(sql.Identifier, column_names))
        )


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
    weight: Decimal
    price: Decimal
    departure_date: datetime
    arrival_date: datetime


class DriverPlace(MyModel, BaseModel):
    id: Optional[int] = None
    date_place: date
    car_id: int
    driver_id: int
    plate_number: Optional[str] = None
    fio: Optional[str] = None


class Run(MyModel, BaseModel):
    id: Optional[int] = None
    car_id: int
    driver_id: Optional[int] = None
    invoice_id: int
    date_departure: date
    invoice_document: Optional[str] = None
    waybill: Optional[str] = None
    weight: Optional[Decimal] = Decimal(0)
    _date_arrival: Optional[date] = None
    reg_number: Optional[str] = None
    _reg_date: Optional[date] = None
    acc_number: Optional[str] = None
    _acc_date: Optional[date] = None
    client: Optional[str] = None
    route: Optional[str] = None
    cargo: Optional[str] = None

    @property
    def reg_date(self) -> datetime.date:
        return self._reg_date

    @reg_date.setter
    def reg_date(self, value: str) -> None:
        self._reg_date = datetime.strptime(value, "%Y-%m-%d").date()

    @property
    def acc_data(self) -> datetime.date:
        return self._reg_date

    @acc_data.setter
    def acc_data(self, value: str) -> None:
        self._reg_date = self.str_to_date(value)

    @property
    def date_arrival(self) -> datetime.date:
        return self._reg_date

    @date_arrival.setter
    def date_arrival(self, value: str) -> None:
        self._reg_date = self.str_to_date(value)

    @staticmethod
    def str_to_date(date_str: str) -> datetime.date:
        if date_str:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
