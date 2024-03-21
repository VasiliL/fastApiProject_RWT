from fastapi import APIRouter, UploadFile, HTTPException

from components.datafiles import IncomeDocsDF, ClientDocsDF, RunsDFClientWeight, RunsDFWeight
from components.func import post_multiple_objects, put_multiple_objects

router = APIRouter()


@router.get("/api/documents/")
async def about_section():
    return {"message": "Hello, You are in Documents section."}


@router.put("/api/documents/income_docs/upload_xlsx")
async def income_docs_upload_xlsx(file: UploadFile):
    """
    Загружает файл с информацией о рейсах и их путевых листах и ТН.
    В файле должны быть столбцы: ИД Рейса, ПЛ, ТН

    По данным в файле будут созданы записи о документах ПЛ и ТН.

    Args (necessary all):

    - file (UploadFile): The file to be uploaded.

    Returns (any):

    - List[int|str]: The list of The ID's of the inserted data or strings of errors.
    """
    try:
        documents_runs = IncomeDocsDF(file)
        documents_runs.MAX_FILE_SIZE = 15 * 1024  # 15kB
        documents_items = await documents_runs.objects_list
        result = await post_multiple_objects(documents_items, "runs_documents")
        try:
            conditions = ('id',)
            runs = RunsDFWeight(file)
            runs_items = await runs.objects_list
            await put_multiple_objects(runs_items, "runs", conditions)
        except HTTPException as e:
            return e
    except HTTPException as e:
        return e
    return result


@router.put("/api/documents/outcome_docs/upload_xlsx")
async def outcome_docs_upload_xlsx(file: UploadFile):
    """
    Загружает файл с информацией о документах Заказчику и от Поставщика.
    В файле должны быть столбцы: ИД Рейса, УПД Поставщика, Реестр Заказчику, УПД Заказчику.

    По данным в файле будут созданы записи о документах УПД Поставщика, Реестр Заказчику, УПД Заказчику.


    Args (necessary all):

    - file (UploadFile): The file to be uploaded.

    Returns (any):

    - List[int|str]: The list of The ID's of the inserted data or strings of errors.
    """
    try:
        documents_runs = ClientDocsDF(file)
        documents_runs.MAX_FILE_SIZE = 15 * 1024  # 15kB
        documents_items = await documents_runs.objects_list
        result = await post_multiple_objects(documents_items, "runs_documents")
        try:
            conditions = ('id',)
            runs = RunsDFClientWeight(file)
            runs_items = await runs.objects_list
            await put_multiple_objects(runs_items, "runs", conditions)
        except HTTPException as e:
            return e
    except HTTPException as e:
        return e
    return result
