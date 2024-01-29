from fastapi import FastAPI
from my_psql import cars
import json

app = FastAPI()

TABLES = {'persons': ('_reference300', '_reference155', '_reference89',),
              'cars': ('_reference262', '_reference211', '_reference259'),
              'cargo': ('_reference124',),
              'routes': ('_reference207', '_reference207_vt7419', '_reference225',)}
VIEWS = {'persons': 'persons', 'cars': 'cars', 'cargo': 'cargo', 'routes': 'routes'}


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get('/bi/update_persons')
def update_persons():
    for table in ('_reference300', '_reference155', '_inforg10632'):
        _obj = cars.BItable(table)
        _obj.db1c_sync()
    return {"message": "Persons updated"}


@app.get('/bi/update_cars')
def update_cars():
    for table in ('_reference262', '_reference211', '_reference259'):
        _obj = cars.BItable(table)
        _obj.db1c_sync()
    return {"message": "Cars updated"}


@app.get('/bi/update_data/{data}')
def update_data(data):
    if data in TABLES:
        for table in TABLES[data]:
            _obj = cars.BItable(table)
            _obj.db1c_sync()
        return {"message": f"{data} updated"}
    else:
        return {"message": f"Can not update {data}"}


@app.get('/bi/get/{data}')
def get_data(data):
    if data in VIEWS:
        _obj = cars.BIView(data)
        json_data = json.dumps([dict(row) for row in _obj.get_data()], default=str, ensure_ascii=False)
        return json_data
