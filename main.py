from fastapi import FastAPI
from my_psql import cars
import json

app = FastAPI()

TABLES = {'persons': ('_reference300', '_reference155', '_reference89',),
          'cars': ('_reference262', '_reference211', '_reference259'),
          'cargo': ('_reference124',),
          'routes': ('_reference207', '_reference207_vt7419', '_reference225',),
          'counterparty': ('_reference111',),
          'shipment_requests': ('_document350', '_document350_vt1855', '_document365_vt2454')}
STATIC_VIEWS = {'persons': 'persons', 'cars': 'cars', 'cargo': 'cargo', 'routes': 'routes',
                'counterparty': 'counterparty'}


@app.get("/")
async def root():
    return {"message": "Hello World"}


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
    if data in STATIC_VIEWS:
        _obj = cars.BIView(data)
        json_data = json.dumps([dict(row) for row in _obj.get_data()], default=str, ensure_ascii=False)
        return json_data
    return {"message": "No data"}
