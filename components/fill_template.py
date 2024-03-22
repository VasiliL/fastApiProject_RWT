import os.path

from fillpdf import fillpdfs
from models import documents as doc_model
from routes import front_interaction as front

class pdf_file:
    pass


class TN(pdf_file):
    FIELDS = ['doc_date', 'name', 'shipper', 'client', 'consignee', 'cargo', 'weight', 'fio_and_driver_license',
              'model_and_type', 'car_and_trailer', 'shipper2', 'departure_point', 'doc_date2', 'doc_date4',
              'doc_date3', 'fio', 'weight_arrival', 'fio2', 'client_contract', 'carrier_director', 'client2']

    def __init__(self, run_id: int, doc_name: str):
        self.template_file = os.path.join("template_docs", "tn.pdf")
        self.doc = doc_model.TransportInvoice(run_id=run_id, name=doc_name, doc_type=2)

    async def add_run_data(self):
        run_data = front.get_run(self.doc.run_id)
        self.doc = self.doc.copy(update=(await run_data).dict())

    async def add_invoice_data(self):
        invoice_data = front.get_invoice(self.doc.invoice_id)
        self.doc = self.doc.copy(update=(await invoice_data).dict())

    async def add_car_data(self):
        car_data = front.get_car(self.doc.car_id)
        self.doc = self.doc.copy(update=(await car_data).dict())

    async def add_driver_data(self):
        driver_data = front.get_driver(self.doc.driver_id)
        self.doc = self.doc.copy(update=(await driver_data).dict())

    async def get_data(self):
        await self.add_run_data()
        await self.add_invoice_data()
        await self.add_car_data()
        await self.add_driver_data()
        return self.doc.dict()

    def fill_pdf(self):
        pass

    def remove_filled_file(self):
        pass
