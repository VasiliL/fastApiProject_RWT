import json
from io import StringIO

import pandas as pd

from components.hash_strings import MagOilPassword
import requests

from models.documents import MagOilReport


class MagOilInterface:
    def __init__(self, data: MagOilReport, credentials: MagOilPassword = MagOilPassword()):
        self.__df = None
        self.session = requests.session()
        self.credentials = credentials
        self.login_headers = {
            'x-requested-with': 'XMLHttpRequest',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/81.0.4044.129 Safari/537.36',
        }
        self.data = data
        self.session.headers = self.login_headers

    @property
    def df(self):
        if self.__df is None:
            self.authorize()
            self.__df = self.get_csv()
        return self.__df

    def authorize(self):
        response = self.session.get(f'http://18.215.116.239/cards/group/index/list/clientId/87'
                                    f'?act=auth&login={self.credentials.login}')
        r_j = response.json()
        self.credentials.auth_key = r_j['auth_key']
        self.credentials.salt = r_j['salt']
        creds = self.credentials.crypt_password()
        self.session.get(f'http://18.215.116.239/cards/group/index/list/clientId/87'
                         f'?act=auth&password={creds}&auth_key={self.credentials.auth_key}')

    def get_csv(self):
        csv = self.session.get(f'http://18.215.116.239/cards/report/card/export-csv/'
                               f'datestart/{self.data.start_date.strftime('%d.%m.%Y')}/'
                               f'dateend/{self.data.end_date.strftime('%d.%m.%Y')}/clientId/87')
        csv_decoded = csv.content.decode('utf-8-sig')
        df = pd.read_csv(StringIO(csv_decoded), sep=';')
        return df

    def get_json(self):
        return json.dumps(self.df.fillna('').to_dict(orient="records"), ensure_ascii=False)
