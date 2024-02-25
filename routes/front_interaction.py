from datetime import date
from fastapi import APIRouter, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from database import cars
from models.front_interaction import Car, Person, Invoice, DriverPlace, Run
from psycopg2 import sql
from typing import List, Optional
import routes.func as func
import pandas as pd
import io

router = APIRouter()

TABLES = {"persons": ("_reference300", "_reference155", "_reference89"),
    "cars": ("_reference262", "_reference211", "_reference259"),
    "invoices": ("_document350", "_document350_vt1855", "_document365_vt2454", "_reference124", "_reference207",
                 "_reference207_vt7419", "_reference225", "_reference111", "_reference110")}
STATIC_VIEWS = {"persons", "cars", "cargo", "routes", "counterparty", "invoices", "react_drivers", "react_cars",
                "runs_view"}
MAX_FILE_SIZE = 1 * 1024 * 1024  # 1 MB

@router.get("/")
async def root():
    """
    Typically "Hello World" for testing.
    """
    return {"message": "Hello World"}


@router.patch("/api/update_data/{data}")
def update_data(data: str):
    """
    Синхронизирует данные из таблиц 1С в БД Cars

    Args:

    - data (str): The name of the table to update Can be: 'persons', 'cars', 'invoices'.

    Returns:

    - JSON: A message indicating the status of the update.
    """
    _data = str(data).lower()
    if _data in TABLES:
        for table in TABLES[_data]:
            _obj = cars.CarsTable(table)
            with _obj:
                _obj.sync()
        return JSONResponse(status_code=200, content={"message": f"{_data} updated"})
    else:
        return JSONResponse(status_code=400, content={"message": f"Can not update {_data}"})


@router.get("/api/cars", response_model=List[Car])
async def get_cars():
    """
    Возвращает список машин.

    Args: None

    Returns:

    - List[Car]: A list of Car objects.
    """
    view = "cars"
    cl = Car
    where = sql.SQL(
        """where car_type in ('Грузовые автомобили тягачи седельные', 'Тягачи седельные 6х4',
     'Грузовые самосвалы 8х4')"""
    )
    return await func.get_view_data(view, cl, where)


@router.get("/api/drivers", response_model=List[Person])
async def get_drivers():
    """
    Возвращает список водителей.

    Args: None

    Returns:

    - List[Person]: A list of Person objects.
    """
    view = "persons"
    cl = Person
    where = sql.SQL("where position = {}").format(sql.Literal("Водитель-экспедитор"))
    return await func.get_view_data(view, cl, where)


@router.get("/api/invoices", response_model=List[Invoice])
async def get_invoices(day: Optional[date] = None):
    """
    Возвращает список Заявок на перевозку, актуальных на дату запроса.

    Args:

    - day (Optional[date]): The date to retrieve the invoices for.

    Returns:

    - List[Invoice]: A list of Person objects.
    """
    view = "invoices"
    cl = Invoice
    day = day or date.today()
    where = (
        sql.SQL(" WHERE {day} between {departure_date} and {arrival_date}").format(
            departure_date=sql.Identifier("departure_date"),
            arrival_date=sql.Identifier("arrival_date"),
            day=sql.Literal(day.strftime("%Y-%m-%d")),
        )
        if day
        else None
    )
    return await func.get_view_data(view, cl, where)


@router.get("/api/drivers_place", response_model=List[DriverPlace])
async def get_drivers_place(start_day: date, end_day: date):
    """
    Возвращает расстановку водителей на машины для всех дат, указанных в запросе.

    Args:

    - start_day (date): The start date of the range.
    - end_day (date): The end date of the range.

    Returns:

    - List[DriverPlace]: A list of DriverPlace objects.
    """
    view = "drivers_place"
    cl = DriverPlace
    where = sql.SQL("WHERE date between {start_date} and {end_date}").format(
        start_date=sql.Literal(start_day.strftime("%Y-%m-%d")),
        end_date=sql.Literal(end_day.strftime("%Y-%m-%d")),
    )
    return await func.get_view_data(view, cl, where)


@router.post("/api/drivers_place")
def set_drivers_place(data: DriverPlace) -> str | int:
    """
    Создает запись в таблице расстановки водителей на машины.

    Args (necessary all):

    - date (date): The date on which the record will be created.
    - car_id (int): The Car ID receive from cars route.
    - driver_id (int): The Driver ID receive from drivers route.

    Returns (any):

    - int: The ID of the inserted data
    - str: The error message
    """
    columns = ("date_place", "driver_id", "car_id")
    columns_data = dict(zip(columns, [data.date_place, data.driver_id, data.car_id]))
    _obj = cars.CarsTable("drivers_place_table")
    with _obj:
        result = _obj.insert_data(columns_data)
    return result if isinstance(result, str) else result[0]["lastrowid"][0]


async def check_xlsx_file(file: UploadFile) -> bool:
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="File must be in xlsx format")
    if file.size > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File is too big")
    return True


async def get_df(file):
    content = await file.read()
    return pd.read_excel(io.BytesIO(content))


@router.post('/api/drivers_place/upload_xlsx')
async def driver_places_upload_xlsx(file: UploadFile):
    """
    Загружает файл с расстановкой водителей на машины.

    Args (necessary all):

    - file (UploadFile): The file to be uploaded.

    Returns (any):

    - List[int|str]: The list of The ID's of the inserted data or strings of errors.
    """
    if check_xlsx_file(file):
        try:
            result = []
            df = await get_df(file)
            df = df.rename(columns={"Дата": "date_place", "Водитель": "driver_id", "Машина": "car_id"})
            df = df.astype({"date_place": "datetime64[ns]", "driver_id": "int64", "car_id": "int64"})
            df['date_place'] = df['date_place'].dt.strftime('%Y-%m-%d')
            df = df[["date_place", "driver_id", "car_id"]]
            _obj = cars.CarsTable("drivers_place_table")
            with _obj:
                for index, row in df.iterrows():
                    _check = DriverPlace(**row.to_dict())
                    result.append(_obj.insert_data(row.to_dict()))
        except KeyError as e:
            return HTTPException(status_code=400, detail=f"Должны быть столбцы: Дата, Водитель, Машина: {e}")
        except ValueError as e:
            return HTTPException(status_code=400, detail=f"Ошибка в данных (Дату указать в формате дд.мм.гггг, "
                                                         f"идентификаторы машины и водителя - целые числа): {e}")
        return result


@router.put("/api/drivers_place")
async def put_drivers_place(data: DriverPlace) -> str | bool:
    """
    Обновляет запись в таблице расстановки водителей на машины.

    Args (necessary all):

    - id (int): The ID of the record to be updated.
    - date (date): The date on which the record will be created.
    - car_id (int): The Car ID receive from cars route.
    - driver_id (int): The Driver ID receive from drivers route.

    Returns (any):

    - bool: True if successful, False if request does not change anything.
    - str: The error message.
    """
    columns = ("_date", "driver", "car")
    condition_columns = ("id",)
    columns_data = dict(zip(columns, [data.date, data.driver_id, data.car_id]))
    condition_data = dict(zip(condition_columns, [data.id,]))
    _obj = cars.CarsTable("drivers_place_table")
    with _obj:
        result = _obj.update_data(columns_data, condition_data)
    return True if isinstance(result, list) else result


@router.delete("/api/drivers_place")
async def delete_drivers_place(data: int) -> str | bool:
    """
    Удаляет запись в таблице расстановки водителей на машины.

    Args (necessary all):

    - id (int): The ID of the record to be deleted.
    Returns (any):

    - bool: True if successful, False if request does not change anything.
    - str: The error message.
    """
    condition_columns = ("id",)
    condition_data = dict(zip(condition_columns, [data,]))
    _obj = cars.CarsTable("drivers_place_table")
    with _obj:
        result = _obj.delete_data(condition_data)
    return result if isinstance(result, str) else bool(result[0]["rowcount"])


@router.get("/api/runs", response_model=List[Run])
async def get_runs(start_day: date, end_day: date):
    """
    Возвращает список рейсов автомобилей для всех дат, указанных в запросе.

    Args (necessary all):

    - start_day (date): The start date of the range.
    - end_day (date): The end date of the range.

    Returns (any):

    - List[Run]: A list of Run objects.
    """
    view = "runs_view"
    cl = Run
    where = sql.SQL("WHERE date_departure between {start_date} and {end_date}").format(
        start_date=sql.Literal(start_day.strftime("%Y-%m-%d")),
        end_date=sql.Literal(end_day.strftime("%Y-%m-%d")),
    )
    return await func.get_view_data(view, cl, where)


@router.post("/api/runs")
async def post_runs(data: Run) -> str | int:
    """
    Создает запись в таблице рейсов.

    Args (necessary all):

    - date (date): The date on which the record will be created.
    - car_id (int): The Car ID receive from cars route.
    - invoice_id (int): The Invoice ID receive from invoices route.

    Args (optional any):

    - weight (Decimal): The weight of the cargo.

    Returns (any):

    - int: The ID of the inserted data
    - str: The error message
    """
    columns = ("weight", "date_departure", "car", "invoice")
    columns_data = dict(zip(columns, [data.weight, data.date_departure, data.car_id, data.invoice_id]))
    _obj = cars.CarsTable("runs")
    with _obj:
        result = _obj.insert_data(columns_data)
    return result if isinstance(result, str) else result[0]["lastrowid"][0]


@router.put("/api/runs")
async def put_runs(data: Run):
    """
    Изменяет запись в таблице рейсов.

    Args (necessary all):

    - id (int): The ID of the record to be updated.
    - date_departure (date): The new date of the record.
    - car_id (int): The Car ID receive from cars route.
    - invoice_id (int): The Invoice ID receive from invoices route.

    Args (optional any):

    - driver_id (int): The Driver ID receive from drivers route.
    - weight (Decimal): The weight of the cargo.
    - waybill (str): The waybill number.
    - invoice_document (str): The invoice document number.
    - date_arrival (date): The date of run arrival.
    - reg_number (str): The accountancy registry number.
    - reg_date (date): The accountancy registry date.
    - acc_number (str): The accountancy invoice number.
    - acc_date (date): The accountancy invoice date.

    Returns (any):

    - bool: True if successful, False if request does not change anything.
    - str: The error message.
    """
    columns = ("date_departure", "car_id", "invoice_id", "driver_id", "weight", "waybill", "invoice_document",
               "date_arrival", "reg_number", "reg_date", "acc_number", "acc_date")
    condition_columns = ("id",)
    columns_data = dict(zip(columns, [data.date_departure, data.car_id, data.invoice_id, data.driver_id, data.weight,
                                        data.waybill, data.invoice_document, data.date_arrival, data.reg_number,
                                        data.reg_date, data.acc_number, data.acc_date]))
    condition_data = dict(zip(condition_columns, [data.id,]))
    _obj = cars.CarsTable("runs")
    with _obj:
        result = _obj.update_data(columns_data, condition_data)
    return True if isinstance(result, list) else result


@router.delete("/api/runs")
async def delete_runs(data: int):
    """
    Удаляет запись в таблице рейсов.

    Args (necessary all):

    - id (int): The ID of the record to be deleted.

    Returns (any):

    - bool: True if successful, False if request does not change anything.
    - str: The error message.
    """
    condition_columns = ("id",)
    condition_data = dict(zip(condition_columns, [data, ]))
    _obj = cars.CarsTable("runs")
    with _obj:
        result = _obj.delete_data(condition_data)
    return result if isinstance(result, str) else bool(result[0]["rowcount"])
