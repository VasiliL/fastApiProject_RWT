from fastapi import APIRouter
from typing import List
from models.mmk_oracle import Recipe, Invoice, Certificate
from database import cars
from datetime import date
from psycopg2 import sql
import routes.func as func

router = APIRouter()


@router.get('/api/mmk')
async def about_section():
    return {"message": "Hello, You are in MMK section."}


@router.get('/api/mmk/certificate_empty')
async def get_certificate_empty() -> List[date]:
    """Get dates of invoices where certificates have no Yandex disk link.
    It means that the certificate has not downloads from MMK yet."""
    _obj = cars.CarsTable('mmk_oracle_certificate')
    query = sql.SQL('select distinct i.invoice_date from mmk_oracle_certificate c join mmk_oracle_invoices i '
                    'on c.invoice_number = i.invoice_number where c.link is null order by i.invoice_date;')
    with _obj:
        result = _obj.dql_handler(query)
    result = list([i[0] for i in result[0]])
    return result


@router.post('/api/mmk/recipe')
async def post_recipe(data: List[Recipe]):
    _obj = cars.CarsTable('mmk_oracle_recipes')
    result = []
    with _obj:
        for recipe in data:
            _dict = recipe.dict().copy()
            result.append(_obj.insert_data(_dict))
    return result


@router.post('/api/mmk/invoice')
async def post_invoice(data: List[Invoice]):
    _obj = cars.CarsTable('mmk_oracle_invoices')
    result = []
    with _obj:
        for invoice in data:
            _dict = invoice.dict().copy()
            if 'id' in _dict:
                del _dict['id']
            _r = _obj.insert_data(_dict)
            result.append(True if type(_r) is list else _r)
    return result


@router.post('/api/mmk/certificate')
async def post_certificate(data: List[Certificate]):
    _obj = cars.CarsTable('mmk_oracle_certificate')
    result = []
    with _obj:
        for certificate in data:
            _dict = certificate.dict().copy()
            if 'id' in _dict:
                del _dict['id']
            _r = _obj.insert_data(_dict)
            result.append(True if type(_r) is list else _r)
    return result


@router.put('/api/mmk/certificate')
async def put_certificate(data: List[Certificate]):
    _obj = cars.CarsTable('mmk_oracle_certificate')
    columns = ('weight_dry', 'weight_wet', 'link')
    condition_columns = ('certificate_name',)
    result = []
    with _obj:
        for certificate in data:
            columns_data = dict(zip(columns, [certificate.weight_dry, certificate.weight_wet, certificate.link]))
            columns_data = dict({k: v for k, v in columns_data.items() if v is not None})
            condition_data = dict(zip(condition_columns, [certificate.certificate_name, ]))
            try:
                result.append(_obj.update_data(columns_data, condition_data))
            except IndexError as e:
                if str(e) == 'list index out of range':
                    result.append('No invoice for Certificate')
    return result
