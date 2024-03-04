from typing import List
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from models.mmk_oracle import Invoice, Certificate
from database import sql_handler
from datetime import date
from psycopg2 import sql
from components.func import post_multiple_objects, put_multiple_objects


router = APIRouter()


@router.get("/api/mmk")
async def about_section():
    return {"message": "Hello, You are in MMK section."}


@router.get("/api/mmk/certificate_empty")
async def get_certificate_empty() -> List[date]:
    """Get dates of invoices where certificates have no Yandex disk link.
    It means that the certificate has not downloads from MMK yet."""
    _obj = sql_handler.CarsTable("mmk_oracle_certificate")
    query = sql.SQL(
        "select distinct i.invoice_date from mmk_oracle_certificate c join mmk_oracle_invoices i "
        "on c.invoice_number = i.invoice_number where c.link is null order by i.invoice_date;"
    )
    with _obj:
        result = _obj.dql_handler(query)
    result = list([i[0] for i in result[0]])
    return result


@router.post("/api/mmk/invoice")
async def post_invoice(data: List[Invoice]):
    return await post_multiple_objects(data, "mmk_oracle_invoices")


@router.post("/api/mmk/certificate")
async def post_certificate(data: List[Certificate]):
    return await post_multiple_objects(data, "mmk_oracle_certificate")


@router.put("/api/mmk/certificate")
async def put_certificate(data: List[Certificate]):
    try:
        return await put_multiple_objects(data, "mmk_oracle_certificate", ("certificate_name",))
    except TypeError as e:
        if "'NoneType' object is not" in str(e):
            return JSONResponse(status_code=200, content='No data to update.')
