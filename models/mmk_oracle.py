from pydantic import BaseModel
from datetime import date
from typing import Optional
from decimal import Decimal


class Invoice(BaseModel):
    id: Optional[int] = None
    contract: str
    offer: str
    factory: str
    invoice_number: str
    invoice_date: date
    invoice_create_date: date
    str_number: str
    cargo_name: str
    car: str
    cargo_attribute: str
    cargo_tests: Optional[str] = None
    currency: str
    price: Decimal
    amount: Decimal
    price_sum: Decimal
    vat_sum: Decimal


class Certificate(BaseModel):
    id: Optional[int] = None
    certificate_name: str
    date_cert: Optional[date] = None
    invoice_number: Optional[str] = None
    weight_dry: Optional[Decimal] = None
    weight_wet: Optional[Decimal] = None
    link: Optional[str] = None
