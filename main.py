from fastapi import FastAPI
from my_psql import cars

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get('/nsi/db1c_sync/{name}')
async def db1c_sync(name: str):
    if name in ['_reference300', '_reference155']:
        obj = cars.BItable(name)
        sync_status = obj.db1c_sync()
        if sync_status:
            return {"message": f"Table {name} is synced"}
        return {"message": f"Unable to sync table {name}"}
    else:
        return {"message": f"Table {name} not found"}
