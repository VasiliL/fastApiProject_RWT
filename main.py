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
    for table in ('_reference262', ):
        _obj = cars.BItable(table)
        _obj.db1c_sync()
    return {"message": "Cars updated"}
