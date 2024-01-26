from fastapi import FastAPI
from my_psql import cars

app = FastAPI()


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
    tables = {'persons': ('_reference300', '_reference155', '_reference89',),
              'cars': ('_reference262', '_reference211', '_reference259'),
              'cargo': ('_reference124',),
              'routes': ('_reference207', '_reference207_vt7419', '_reference225',)}
    if data in tables:
        for table in tables[data]:
            _obj = cars.BItable(table)
            _obj.db1c_sync()
        return {"message": f"{data} updated"}
    else:
        return {"message": f"Can not update {data}"}
