from fastapi import APIRouter
from typing import List
from models.mmk_oracle import Recipe, Invoice, Certificate
from database import cars
import routes.func as func

router = APIRouter()


@router.get('/api/mmk')
async def about_section():
    return {"message": "Hello, You are in MMK section."}


@router.get('/api/mmk/recipe')
async def get_recipe():
    return {"message": "Hello World"}


@router.get('/api/mmk/invoice')
async def about():
    return {"message": "Hello World"}


@router.get('/api/mmk/certificate')
async def about():
    return {"message": "Hello Wowerrld"}


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
            del _dict['id']
            result.append(_obj.insert_data(_dict))
    return result


@router.post('/api/mmk/certificate')
async def post_certificate(data: Certificate):
    pass
