from datetime import date
from fastapi import APIRouter, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from database import sql_handler
from models.front_interaction import Car, Person, Invoice, DriverPlace, Run
from psycopg2 import sql
from typing import List, Optional
import components.func as func
from components.datafiles import DriverPlacesDF, RunsDF
from components.func import post_multiple_objects, put_multiple_objects

router = APIRouter()

TABLES = {"persons": ("_reference300", "_reference155", "_reference89", "_inforg13301", "_inforg10632",
                      "_reference228", "_reference179", "_reference75"),
          "cars": ("_reference262", "_reference211", "_reference259", "_inforg13254"),
          "invoices": ("_document350", "_document350_vt1855", "_document365_vt2454", "_reference124", "_reference207",
                       "_reference207_vt7419", "_reference225", "_reference111", "_reference110", "_reference128",
                       "_document350_vt1893", "_reference88")}
STATIC_VIEWS = {"persons", "cars", "cargo", "routes", "counterparty", "invoices", "react_drivers", "react_cars",
                "runs_view"}


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
            with sql_handler.CarsTable(table) as _obj:
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
    return await func.get_view_data(view, cl)


@router.get("/api/car_by_id", response_model=Optional[Car])
async def get_car(car_id: int):
    """
    Возвращает машину по ИД.

    Args: car_id

    Returns:

    - Car: A list of Car objects.
    """
    view = "cars"
    cl = Car
    car_id = car_id if isinstance(car_id, int) else None
    if car_id:
        where = sql.SQL(" WHERE id = {car_id}").format(car_id=sql.Literal(car_id))
        result = await func.get_view_data(view, cl, where)
        return result[0]


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
    where = sql.SQL("where position = {} or position is null").format(sql.Literal('Водитель-экспедитор'))
    return await func.get_view_data(view, cl, where)


@router.get("/api/driver_by_id", response_model=Optional[Person])
async def get_driver(driver_id: int):
    """
    Возвращает водителя по ИД.

    Args: driver_id

    Returns:

    - Person: The Person objects.
    """
    view = "persons"
    cl = Person
    driver_id = driver_id if isinstance(driver_id, int) else None
    if driver_id:
        where = sql.SQL(" WHERE id = {driver_id}").format(driver_id=sql.Literal(driver_id))
        result = await func.get_view_data(view, cl, where)
        return result[0]


@router.get("/api/invoices", response_model=Optional[List[Invoice]])
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


@router.get("/api/invoice_by_id", response_model=Optional[Invoice])
async def get_invoice(invoice_id: int):
    """
    Возвращает Заявку на перевозку по ИД.

    Args:

    - invoice_id: The Invoice ID.

    Returns:

    - Invoice: The Invoice objects.
    """
    view = "invoices"
    cl = Invoice
    invoice_id = invoice_id if isinstance(invoice_id, int) else None
    if invoice_id:
        where = sql.SQL(" WHERE id = {invoice_id}").format(invoice_id=sql.Literal(invoice_id))
        result = await func.get_view_data(view, cl, where)
        return result[0]


@router.get("/api/drivers_place", response_model=Optional[List[DriverPlace]])
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
    where = sql.SQL("WHERE date_place between {start_date} and {end_date}").format(
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
    with sql_handler.CarsTable("drivers_place_table") as _obj:
        result = _obj.insert_data(columns_data)
    return result if isinstance(result, str) else result[0]["lastrowid"][0]


@router.post('/api/drivers_place/upload_xlsx')
async def post_driver_places_upload_xlsx(file: UploadFile):
    """
    Загружает файл с расстановкой водителей на машины. В файле должны быть столбцы: Дата, ИД Водителя, ИД Машины.
    Дополнительные столбцы допускаются, но не обрабатываются.
    По данным в файле будут созданы новые записи.

    Args (necessary all):

    - file (UploadFile): The file to be uploaded.

    Returns (any):

    - List[int|str]: The list of The ID's of the inserted data or strings of errors.
    """
    try:
        places = DriverPlacesDF(file, method='POST')
        places.MAX_FILE_SIZE = 15 * 1024  # 15kB
        places_items = await places.objects_list
        result = await post_multiple_objects(places_items, "drivers_place_table")
    except HTTPException as e:
        return e
    return result


@router.put("/api/drivers_place")
async def put_drivers_place(data: DriverPlace) -> str | bool:
    """
    Обновляет запись в таблице расстановки водителей на машины.
    В файле должны быть столбцы: ИД, Дата, ИД Водителя, ИД Машины.
    Дополнительные столбцы допускаются, но не обрабатываются.
    По данным в файле будут внесены изменения в текущие записи.

    Args (necessary all):

    - id (int): The ID of the record to be updated.
    - date_place (date): The date on which the record will be created.
    - car_id (int): The Car ID receive from cars route.
    - driver_id (int): The Driver ID receive from drivers route.

    Returns (any):

    - bool: True if successful, False if request does not change anything.
    - str: The error message.
    """
    columns = ("date_place", "driver_id", "car_id")
    condition_columns = ("id",)
    columns_data = dict(zip(columns, [data.date_place, data.driver_id, data.car_id]))
    condition_data = dict(zip(condition_columns, [data.id, ]))
    with sql_handler.CarsTable("drivers_place_table") as _obj:
        result = _obj.update_data(columns_data, condition_data)
    return True if isinstance(result, list) else result


@router.put('/api/drivers_place/upload_xlsx')
async def put_driver_places_upload_xlsx(file: UploadFile):
    """
    Загружает файл с расстановкой водителей на машины. В файле должны быть столбцы: ID, Дата, Водитель, Машина.
    По данным в файле будут изменены записи с соответствующим ID на данные из файла.
    Максимальный размер файла для загрузки - 15 кБ

    Args (necessary all):

    - file (UploadFile): The file to be uploaded.

    Returns (any):

    - List[int|str]: The list of The ID's of the inserted data or strings of errors.
    """
    conditions = ('id',)
    try:
        places = DriverPlacesDF(file, method='PUT')
        places.MAX_FILE_SIZE = 15 * 1024  # 15kB
        places_items = await places.objects_list
        result = await put_multiple_objects(places_items, "drivers_place_table", conditions)
    except HTTPException as e:
        return e
    return result


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
    condition_data = dict(zip(condition_columns, [data, ]))
    with sql_handler.CarsTable("drivers_place_table") as _obj:
        result = _obj.delete_data(condition_data)
    return result if isinstance(result, str) else bool(result[0]["rowcount"])


@router.get("/api/runs", response_model=Optional[List[Run]])
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
    result = await func.get_view_data(view, cl, where)
    return result


@router.get("/api/run_by_id", response_model=Optional[Run])
async def get_run(run_id: int):
    """
    Возвращает список рейс по его ИД.

    Args (necessary all):

    - run_id (int): The ID of the run.

    Returns (any):

    - Run: A list of Run objects.
    """
    view = "runs_view"
    cl = Run
    where = sql.SQL(" WHERE id = {run_id}").format(run_id=sql.Literal(run_id))
    result = await func.get_view_data(view, cl, where)
    return result[0]


@router.post("/api/runs")
async def post_runs(data: Run) -> str | int | List[str | int]:
    """
    Создает запись или записи в таблице рейсов.

    Args (necessary all):

    - date (date): The date on which the record will be created.
    - car_id (int | List[int]): The Car ID or many of them, receive from cars route.
    - invoice_id (int): The Invoice ID receive from invoices route.

    Args (optional any):

    - weight (Decimal): The weight of the cargo.

    Returns (any):

    - int | List[int]: The ID of the inserted data of one record or many of them.
    - str: The error message
    """
    columns = ("weight", "date_departure", "car_id", "invoice_id")
    columns_data = dict(zip(columns, [data.weight, data.date_departure, data.car_id, data.invoice_id]))
    with sql_handler.CarsTable("runs") as _obj:
        if isinstance(data.car_id, list):
            result = []
            for car_id in data.car_id:
                columns_data["car_id"] = car_id
                _result = _obj.insert_data(columns_data)
                result.append(_result if isinstance(_result, str) else _result[0]["lastrowid"][0])
            return result
        else:
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
    condition_data = dict(zip(condition_columns, [data.id, ]))
    with sql_handler.CarsTable("runs") as _obj:
        result = _obj.update_data(columns_data, condition_data)
    return True if isinstance(result, list) else result


# @router.post('/api/runs/upload_xlsx')
# async def runs_upload_xlsx(file: UploadFile):
#     """
#     Загружает краткую версию файла с Рейсами.
#     В файле должны быть столбцы: Дата отправления, ИД Машины, ИД Заявки, Вес_погрузка (опционально).
#     По данным в файле будут созданы новые записи.
#
#     Args (necessary all):
#
#     - file (UploadFile): The file to be uploaded.
#
#     Returns (any):
#
#     - List[int|str]: The list of The ID's of the inserted data or strings of errors.
#     """
#     try:
#         runs = RunsDF(file, method='POST')
#         runs.MAX_FILE_SIZE = 15 * 1024  # 15kB
#         runs_items = await runs.objects_list
#         result = await post_multiple_objects(runs_items, "runs")
#     except HTTPException as e:
#         return e
#     return result


# @router.put('/api/runs/upload_xlsx')
# async def runs_upload_xlsx(file: UploadFile):
#     """
#     Загружает полную версию файла с Рейсами.
#     В файле должны быть столбцы: ИД, Дата отправления, ИД Машины, ИД Заявки, Вес_погрузка, Дата прибытия, Вес_выгрузка,
#     ИД Водителя.
#     По данным в файле будут изменены записи с соответствующим ID на данные из файла.
#     Максимальный размер файла для загрузки - 150 кБ
#
#     Args (necessary all):
#
#     - file (UploadFile): The file to be uploaded.
#
#     Returns (any):
#
#     - List[int|str]: The list of The ID's of the inserted data or strings of errors.
#     """
#
#     conditions = ('id',)
#     try:
#         runs = RunsDF(file, method='PUT')
#         runs.MAX_FILE_SIZE = 150 * 1024  # 150kB
#         runs_items = await runs.objects_list
#         result = await put_multiple_objects(runs_items, "runs", conditions)
#     except HTTPException as e:
#         return e
#     return result


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
    _obj = sql_handler.CarsTable("runs")
    with _obj:
        result = _obj.delete_data(condition_data)
    return result if isinstance(result, str) else bool(result[0]["rowcount"])
