from typing import List, Optional

import pydantic
from fastapi import UploadFile, HTTPException
from abc import ABC
import pandas as pd
import io

from models.documents import Document
from models.front_interaction import MyModel, DriverPlace, Run, RunUpdaterClientWeight, RunUpdaterWeight


class FileXLSX(ABC):
    MAX_FILE_SIZE: int = 1 * 1024 * 1024  # 1 MB

    def __init__(self):
        self.raw_df = None
        self.__df = None
        self.__objects_list = None
        self.file = None
        self.model: Optional[MyModel] = None
        self.cleaned_df = False

    @property
    async def df(self) -> pd.DataFrame:
        if self.__df is None:
            if self.raw_df is None:
                self.raw_df = await self.get_df(self.file)
            self.__df = await self.sanityze_df(self.raw_df)
        return self.__df

    # async def set_df(self, value: pd.DataFrame):
    #     if not self.cleaned_df:
    #         self.raw_df = value
    #         self.__df = await self.sanityze_df(value)
    #     else:
    #         self.__df = value

    @property
    async def objects_list(self) -> List[MyModel]:
        if self.__objects_list is None:
            self.__objects_list = await self.create_models_from_dataframe(self.model, await self.df)
        return self.__objects_list

    # @objects_list.setter
    # async def objects_list(self, value: List[MyModel]):
    #     self.__df = value

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
            return pd.read_excel(io.BytesIO(content), engine='openpyxl')


class DriverPlacesDF(FileXLSX, ABC):
    def __init__(self, file: UploadFile, method: str = 'POST'):
        super().__init__()
        self.file = file
        self.model = DriverPlace
        self.method = method

    async def sanityze_df(self, df: pd.DataFrame):
        message = "Должны быть столбцы: Дата, ИД Водителя, ИД Машины"
        try:
            match self.method:
                case 'POST':
                    df = df.rename(columns={"Дата": "date_place", "ИД Водителя": "driver_id", "ИД Машины": "car_id"})
                    df = df.dropna(subset=["date_place", "driver_id", "car_id"], how="any")
                    df = df.astype({"date_place": "datetime64[ns]", "driver_id": "int64", "car_id": "int64"})
                    df['date_place'] = df['date_place'].dt.strftime('%Y-%m-%d')
                    df = df[["date_place", "driver_id", "car_id"]]
                case 'PUT':
                    message += ", ИД"
                    df = df.rename(columns={"ИД": "id", "Дата": "date_place", "ИД Водителя": "driver_id",
                                            "ИД Машины": "car_id"})
                    df = df.dropna(subset=["id", "date_place", "driver_id", "car_id"], how="any")
                    df = df.astype({"id": "int64", "date_place": "datetime64[ns]", "driver_id": "int64",
                                    "car_id": "int64"})
                    df['date_place'] = df['date_place'].dt.strftime('%Y-%m-%d')
                    df = df[["id", "date_place", "driver_id", "car_id"]]
            self.cleaned_df = True
            return df
        except KeyError:
            raise HTTPException(status_code=400, detail=message)
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
                    df = df.rename(columns={"Дата отправления": "date_departure", "ИД Машины": "car_id",
                                            "ИД Заявки": "invoice_id", "Вес_погрузка": "weight"})
                    df = df.astype({"date_departure": "datetime64[ns]", "car_id": "int64", "invoice_id": "int64",
                                    "weight": "float64"})
                    df = df.dropna(subset=["date_departure", "car_id", "invoice_id"], how="any")
                    df = df[["date_departure", "car_id", "invoice_id", "weight"]]
                case 'PUT':
                    df = df.rename(columns={"ИД Рейса": "id", "Дата отправления": "date_departure",
                                            "ИД Машины": "car_id", "ИД Заявки": "invoice_id", "Вес_погрузка": "weight",
                                            "Дата прибытия": "date_arrival", "Вес_выгрузка": "weight_arrival",
                                            "ИД Водителя": "driver_id", "Вес_погрузка_клиент": "client_weight",
                                            "Вес_выгрузка_клиент": "client_weight_arrival"}).dropna(subset=["id"],
                                                                                                    how="any")
                    df = df.astype({"id": "int64", "date_departure": "datetime64[ns]", "car_id": "int64",
                                    "invoice_id": "int64", "weight": "float64", "date_arrival": "datetime64[ns]",
                                    "weight_arrival": "float64", "driver_id": "int64", "client_weight": "float64",
                                    "client_weight_arrival": "float64"})
                    df = df.dropna(subset=["date_departure", "car_id", "invoice_id"], how="any")
                    df['date_departure'] = df['date_departure'].dt.strftime('%Y-%m-%d')
                    df['date_arrival'] = df['date_arrival'].dt.strftime('%Y-%m-%d') if not df['date_arrival'].empty \
                        else pd.NA
                    df = df[["id", "date_departure", "car_id", "invoice_id", "weight", "date_arrival",
                             "weight_arrival", "driver_id", "client_weight", "client_weight_arrival"]]
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400,
                                detail=f"Должны быть столбцы: Дата отправления, Машина, Заявка, Вес: {e}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка в данных (Дату указать в формате дд.мм.гггг, "
                                                        f"идентификаторы машины и заявки - целые числа, "
                                                        f"вес - число с разделителем-точкой): {e}")


class RunsDFClientWeight(FileXLSX, ABC):
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.raw_df = df
        self.model = RunUpdaterClientWeight

    async def sanityze_df(self, df: pd.DataFrame):
        try:
            df = df.rename(columns={"ИД Рейса": "id", "Вес_погрузка_клиент": "client_weight",
                                    "Вес_выгрузка_клиент": "client_weight_arrival"}).dropna(subset=["id"], how="any")
            df = df.astype({"id": "int64", "client_weight": "float64", "client_weight_arrival": "float64"})
            df = df[["id", "client_weight", "client_weight_arrival"]]
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Должны быть столбцы: ИД Рейса, Вес_погрузка_клиент, "
                                                        f"Вес_выгрузка_клиент: {e}")


class RunsDFWeight(FileXLSX, ABC):
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.raw_df = df
        self.model = RunUpdaterWeight

    async def sanityze_df(self, df: pd.DataFrame):
        try:
            df = df.rename(columns={"ИД Рейса": "id", "Вес_погрузка": "weight",
                                    "Вес_выгрузка": "weight_arrival",
                                    "Дата отправления": "date_departure",
                                    "Дата прибытия": "date_arrival"}).dropna(subset=["id"], how="any")
            df = df.astype({"id": "int64", "weight": "float64", "weight_arrival": "float64",
                            "date_departure": "datetime64[ns]", "date_arrival": "datetime64[ns]"})
            df = df[["id", "weight", "weight_arrival", "date_departure", "date_arrival"]]
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Должны быть столбцы: ИД Рейса, Вес_погрузка, "
                                                        f"Вес_выгрузка, Дата отправления, Дата прибытия: {e}")


class DocumentsDF(FileXLSX, ABC):
    DOC_TYPES = {"ТН": 2, "ПЛ": 1, "Реестр Перевозчика": 6, "УПД Перевозчика": 9, "УПД Поставщика": 8,
                 "Реестр Заказчику": 4, "УПД Заказчику": 7}

    def __init__(self, file: UploadFile):
        super().__init__()
        self.file = file
        self.model = Document

    @classmethod
    def set_doc_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        for key, value in cls.DOC_TYPES.items():
            df.loc[df['doc_type'] == key, 'doc_type'] = value
        return df

    @classmethod
    def melt_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.melt(id_vars=['run_id']).dropna(subset=["value"], how="any")
        df = df.rename(columns={"run_id": "run_id", "value": "name", "variable": "doc_type"})
        df = df[df['name'] != 'nan']
        return cls.set_doc_types(df)

    @classmethod
    def set_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        df_copy = df_copy.drop(['run_id'], axis=1)
        df_copy = df_copy.astype(str)
        df_copy = df_copy.map(lambda x: x.replace('.0', ''))
        df_copy['run_id'] = df['run_id'].astype('int64')
        return df_copy


class IncomeDocsDF(DocumentsDF, FileXLSX, ABC):
    async def sanityze_df(self, df: pd.DataFrame):
        try:
            df = df.rename(columns={"ИД Рейса": "run_id"}).dropna(subset=["run_id"], how="any")
            df = df[["run_id", "ПЛ", "ТН", "Реестр Перевозчика", "УПД Перевозчика"]]
            df = self.melt_df(self.set_types(df))
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400,
                                detail=f"Должны быть столбцы: ИД Рейса, ПЛ, ТН, Реестр Перевозчика, "
                                       f"УПД Перевозчика: {e}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка в данных (Идентификатор рейса - число, ПЛ, ТН, "
                                                        f"Реестр Перевозчика, УПД Перевозчика - текст): {e}")


class ClientDocsDF(DocumentsDF, FileXLSX, ABC):
    async def sanityze_df(self, df: pd.DataFrame):
        try:
            df = df.rename(columns={"ИД Рейса": "run_id"}).dropna(subset=["run_id"], how="any")
            df = df[["run_id", "УПД Поставщика", "Реестр Заказчику", "УПД Заказчику"]]
            df = self.melt_df(self.set_types(df))
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400,
                                detail=f"Должны быть столбцы: ИД Рейса, УПД Поставщика, Реестр Заказчику, "
                                       f"УПД Заказчику: {e}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка в данных (Идентификатор рейса - число, УПД Поставщика,"
                                                        f" Реестр Заказчику, УПД Заказчику - текст): {e}")
