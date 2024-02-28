from typing import List, Optional

import pydantic
from fastapi import UploadFile, HTTPException
from abc import ABC
import pandas as pd
import io
from models.front_interaction import MyModel, DriverPlace, Run


class FileXLSX(ABC):
    MAX_FILE_SIZE: int = 1 * 1024 * 1024  # 1 MB

    def __init__(self):
        self.__df = None
        self.__objects_list = None
        self.file = None
        self.model: Optional[MyModel] = None
        self.cleaned_df = False

    @property
    async def df(self) -> pd.DataFrame:
        if self.__df is None:
            self.__df = await self.sanityze_df(await self.get_df(self.file))
        return self.__df

    @df.setter
    async def df(self, value: pd.DataFrame):
        self.__df = value

    @property
    async def objects_list(self) -> List[MyModel]:
        if self.__objects_list is None:
            self.__objects_list = await self.create_models_from_dataframe(self.model, await self.df)
        return self.__objects_list

    @objects_list.setter
    async def objects_list(self, value: List[MyModel]):
        self.__df = value

    async def sanityze_df(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    async def create_models_from_dataframe(self, model_class, df: pd.DataFrame = None) -> List[MyModel]:
        df = await self.df if df is None else df
        result = []
        try:
            for _, row in df.iterrows():
                row = row.replace("nan", pd.NA).dropna()
                result.append(model_class(**row.to_dict()))
        except pydantic.ValidationError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка в данных: {e}")
        return result

    @classmethod
    async def check_xlsx_file(cls, file) -> bool:
        if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            raise HTTPException(status_code=400, detail="File must be in xlsx format")
        elif file.size > cls.MAX_FILE_SIZE:
            raise HTTPException(status_code=400, detail="File is too big")
        else:
            return True

    @classmethod
    async def get_df(cls, file: UploadFile) -> pd.DataFrame:
        if await cls.check_xlsx_file(file):
            content = await file.read()
            return pd.read_excel(io.BytesIO(content))


class DriverPlacesDF(FileXLSX, ABC):
    def __init__(self, file: UploadFile, method: str = 'POST'):
        super().__init__()
        self.file = file
        self.model = DriverPlace
        self.method = method

    async def sanityze_df(self, df: pd.DataFrame):
        try:
            match self.method:
                case 'POST':
                    df = df.rename(columns={"Дата": "date_place", "Водитель": "driver_id", "Машина": "car_id"})
                    df = df.dropna(subset=["date_place", "driver_id", "car_id"], how="any")
                    df = df.astype({"date_place": "datetime64[ns]", "driver_id": "int64", "car_id": "int64"})
                    df['date_place'] = df['date_place'].dt.strftime('%Y-%m-%d')
                    df = df[["date_place", "driver_id", "car_id"]]
                case 'PUT':
                    df = df.rename(columns={"ID": "id", "Дата": "date_place", "Водитель": "driver_id",
                                            "Машина": "car_id"})
                    df = df.dropna(subset=["id", "date_place", "driver_id", "car_id"], how="any")
                    df = df.astype({"id": "int64", "date_place": "datetime64[ns]", "driver_id": "int64",
                                    "car_id": "int64"})
                    df['date_place'] = df['date_place'].dt.strftime('%Y-%m-%d')
                    df = df[["id", "date_place", "driver_id", "car_id"]]
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Должны быть столбцы: Дата, Водитель, Машина: {e}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка в данных (Дату указать в формате дд.мм.гггг, "
                                                        f"идентификаторы машины и водителя - целые числа): {e}")


class RunsDF(FileXLSX, ABC):
    def __init__(self, file: UploadFile, method: str = 'POST'):
        super().__init__()
        self.file = file
        self.model = Run
        self.method = method

    async def sanityze_df(self, df: pd.DataFrame):
        try:
            match self.method:
                case 'POST':
                    df = df.rename(columns={"Дата отправления": "date_departure", "Машина": "car_id",
                                            "Заявка": "invoice_id", "Вес": "weight"})
                    df = df.astype({"date_departure": "datetime64[ns]", "car_id": "int64", "invoice_id": "int64",
                                    "weight": "float64"})
                    df = df.dropna(subset=["date_departure", "car_id", "invoice_id"], how="any")
                    df = df[["date_departure", "car_id", "invoice_id", "weight"]]
                case 'PUT':
                    df = df.rename(columns={"ID": "id", "Дата отправления": "date_departure", "Машина": "car_id",
                                            "Заявка": "invoice_id", "Вес": "weight", "ПЛ": "waybill",
                                            "ТН": "invoice_document", "Дата прибытия": "date_arrival",
                                            "Номер реестра": "reg_number", "Дата реестра": "reg_date",
                                            "Номер УПД": "acc_number", "Дата УПД": "acc_date"})
                    df = df.astype({"id": "int64", "date_departure": "datetime64[ns]", "car_id": "int64",
                                    "invoice_id": "int64", "weight": "float64", "waybill": "str",
                                    "invoice_document": "str", "date_arrival": "datetime64[ns]", "reg_number": "str",
                                    "reg_date": "datetime64[ns]", "acc_number": "str", "acc_date": "datetime64[ns]"})
                    df = df.dropna(subset=["date_departure", "car_id", "invoice_id"], how="any")
                    df['date_departure'] = df['date_departure'].dt.strftime('%Y-%m-%d')
                    df = df[["id", "date_departure", "car_id", "invoice_id", "weight", "waybill", "invoice_document",
                             "date_arrival", "reg_number", "reg_date", "acc_number", "acc_date"]]
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400,
                                detail=f"Должны быть столбцы: Дата отправления, Машина, Заявка, Вес: {e}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка в данных (Дату указать в формате дд.мм.гггг, "
                                                        f"идентификаторы машины и заявки - целые числа, "
                                                        f"вес - число с разделителем-точкой): {e}")
