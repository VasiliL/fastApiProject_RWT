from pydantic import BaseModel

from models.front_interaction import MyModel


class Document(MyModel, BaseModel):
    name: str
    run_id: int
    doc_type: int


class Waybill(Document):
    def __init__(self, name: str | int, run_id: int, doc_type: int):
        super().__init__(name=str(name), run_id=run_id, doc_type=doc_type)


class TransportInvoice(Document):
    pass
