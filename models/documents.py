from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel

from models.front_interaction import MyModel


class Document(MyModel, BaseModel):
    name: str
    run_id: int
    doc_type: int

    def __init__(self, name: str | int, run_id: int, doc_type: int):
        super().__init__(name=str(name), run_id=run_id, doc_type=doc_type)


class Waybill(Document):
    pass


class TransportInvoice(Document):
    # document attributes, see Document attributes too
    doc_date: Optional[date] = None

    # invoice attributes
    shipper: Optional[str] = None
    client: Optional[str] = None
    consignee: Optional[str] = None
    arrival_point: Optional[str] = None
    cargo: Optional[str] = None
    departure_point: Optional[str] = None
    client_contract: Optional[str] = None

    # run attributes
    invoice_id: Optional[int] = None
    driver_id: Optional[int] = None
    car_id: Optional[int] = None
    weight: Optional[Decimal] = None
    weight_arrival: Optional[Decimal] = None
    date_departure: Optional[date] = None

    # car attributes
    car_model: Optional[str] = None
    car_type: Optional[str] = None
    plate_number: Optional[str] = None
    trailer_description: Optional[str] = None

    # driver attributes
    fio: Optional[str] = None
    driver_license: Optional[str] = None

    # static attributes
    carrier: Optional[str] = "РВ-ТАРИФ ООО ИНН 6679083223"
    carrier_director: Optional[str] = "В. В. Апрельков"


class CargoInvoice(Document):
    pass
