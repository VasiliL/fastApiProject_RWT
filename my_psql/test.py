from my_psql import cars
import json


VIEWS = {'persons': 'persons', 'cars': 'cars', 'cargo': 'cargo', 'routes': 'routes'}


def get_data(data):
    if data in VIEWS:
        _obj = cars.BIView(data)
        json_data = json.dumps([dict(row) for row in _obj.get_data()], default=str, ensure_ascii=False)
        return json_data

