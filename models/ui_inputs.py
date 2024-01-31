from pydantic import BaseModel, computed_field
from datetime import datetime, date


class DriverSet(BaseModel):
    set_date: date
    driver_name: int
    car_name: int


class CarSet(BaseModel):
    set_date: date
    invoice_id: int
    car_name: int


class WaybillSet(BaseModel):
    run_id: int
    waybill: str


class TnSet(BaseModel):
    run_id: int
    tn: str
