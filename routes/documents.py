from fastapi import APIRouter, UploadFile, HTTPException

from components.datafiles import WaybillDF
from components.func import post_multiple_objects

router = APIRouter()


@router.get("/api/documents/")
async def about_section():
    return {"message": "Hello, You are in Documents section."}


@router.put("/api/documents/waybill/upload_xlsx")
async def waybill_tn_upload_xlsx(file: UploadFile):
    """
    Загружает файл с информацией о рейсах и их путевых листах и ТН.
    Также файл содержит перепроверенную и возможно исправленную информацию о весе и дате.
    В файле должны быть столбцы: ИД Рейса, ПЛ, ТН

    По данным в файле будут созданы записи о документах ПЛ и ТН, а также скорректирована информация в рейсах.

    Args (necessary all):

    - file (UploadFile): The file to be uploaded.

    Returns (any):

    - List[int|str]: The list of The ID's of the inserted data or strings of errors.
    """
    try:
        documents_runs = WaybillDF(file)
        documents_runs.MAX_FILE_SIZE = 15 * 1024  # 15kB
        documents_items = await documents_runs.objects_list
        result = await post_multiple_objects(documents_items, "runs_documents")

    except HTTPException as e:
        return e
    return result
