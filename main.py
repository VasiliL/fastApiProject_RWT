from routes.mmk_oracle import router as mmk_router
from routes.front_interaction import router as front
from fastapi import FastAPI


app = FastAPI(redoc_url=None)
app.include_router(mmk_router)
app.include_router(front)
