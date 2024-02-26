from fastapi import UploadFile, HTTPException
from abc import ABC
import pandas as pd
import io


class FileXLSX(ABC):
    MAX_FILE_SIZE: int = 1 * 1024 * 1024  # 1 MB

    def __init__(self):
        self.__df = None
        self.file = None
        self.cleaned_df = False

    @property
    async def df(self) -> pd.DataFrame:
        if self.__df is None:
            self.__df = await self.sanityze_df(await self.get_df(self.file))
        return self.__df

    @df.setter
    async def df(self, value: pd.DataFrame):
        self.__df = value

    async def sanityze_df(self, df: pd.DataFrame):
        raise NotImplementedError

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
    def __init__(self, file: UploadFile):
        super().__init__()
        self.file = file
        self.method = 'POST'

    async def sanityze_df(self, df: pd.DataFrame):
        try:
            df = df.rename(columns={"Дата": "date_place", "Водитель": "driver_id", "Машина": "car_id"})
            df = df.astype({"date_place": "datetime64[ns]", "driver_id": "int64", "car_id": "int64"})
            df['date_place'] = df['date_place'].dt.strftime('%Y-%m-%d')
            df = df[["date_place", "driver_id", "car_id"]]
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Должны быть столбцы: Дата, Водитель, Машина: {e}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка в данных (Дату указать в формате дд.мм.гггг, "
                                                        f"идентификаторы машины и водителя - целые числа): {e}")

    async def sanityze_post(self, df: pd.DataFrame):
        pass

    async def sanityze_put(self, df: pd.DataFrame):
        pass


class RunsDF(FileXLSX, ABC):
    def __init__(self, file: UploadFile):
        super().__init__()
        self.file = file

    async def sanityze_df(self, df: pd.DataFrame):
        try:
            df = df.rename(columns={"Дата": "date_departure", "Машина": "car_id", "Заявка": "invoice_id",
                                    "Вес": "weight"})
            df = df.astype({"date_departure": "datetime64[ns]", "car_id": "int64", "invoice_id": "int64",
                            "weight": "float64"})
            df['date_departure'] = df['date_departure'].dt.strftime('%Y-%m-%d')
            df = df[["date_place", "car_id", "invoice_id", "weight"]]
            self.cleaned_df = True
            return df
        except KeyError as e:
            raise HTTPException(status_code=400,
                                detail=f"Должны быть столбцы: Дата отправления, Машина, Заявка, Вес: {e}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка в данных (Дату указать в формате дд.мм.гггг, "
                                                        f"идентификаторы машины и заявки - целые числа, "
                                                        f"вес - число с разделителем-точкой): {e}")
