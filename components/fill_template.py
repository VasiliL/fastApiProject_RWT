import os.path

from fillpdf import fillpdfs
from models import documents as doc_model
from routes import front_interaction as front


class PdfFile:
    pass


class TN(PdfFile):
    FIELDS = ['date_departure', 'name', 'shipper', 'client', 'consignee', 'cargo', 'weight', 'fio_and_driver_license',
              'car_model_and_car_type', 'plate_number_and_trailer_description', 'copyof_shipper', 'departure_point',
              '_2copyof_date_departure', '_4copyof_date_departure', '_3copyof_date_departure', 'fio', 'arrival_point',
              'copyof_arrival_point', 'weight_arrival', 'copyof_fio', 'copyof_carrier', 'client_contract',
              'carrier_director', 'copyof_client']

    def __init__(self, run_id: int, doc_name: str):
        self.template_file = os.path.join("template_docs", "tn.pdf")
        self.output_file = os.path.join("template_docs", f"{doc_name}.pdf")
        self.doc = doc_model.TransportInvoice(run_id=run_id, name=doc_name, doc_type=2)
        self.data = dict()
        self.content = None

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
        return True

    def fill_pdf(self):
        for item in self.FIELDS:
            if 'copyof_' in item:
                self.data[item] = self.doc.dict()[item[-item[::-1].find('copyof_'[::-1]):]]
            elif '_and_' in item:
                self.data[item] = ' '.join([self.doc.dict()[i] for i in item.split('_and_') if self.doc.dict()[i]])
            else:
                self.data[item] = self.doc.dict()[item]
        fillpdfs.write_fillable_pdf(self.template_file, self.output_file, self.data)

    def get_pdf(self):
        if self.content is None:
            with open(self.output_file, "rb") as file:
                content = file.read()
            os.remove(self.output_file)
        return content
