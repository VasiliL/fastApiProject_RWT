import os

from routes.mmk_oracle import router as mmk_router
from routes.front_interaction import router as front
from routes.documents import router as documents
from routines import schedulers
from fastapi import FastAPI
import time
import multiprocessing
import schedule
import logging
from components.logger_config import configure_logger


tags_metadata = [
    {
        "name": "Site",
        "description": "Operations with front-end site. The **front-end** is cool.",
    },
    {
        "name": "MMK",
        "description": "Operations with MMK Oracle subsystem."
    },
    {
        "name": "Documents",
        "description": "Operations with documents."
    },
]

configure_logger()
app = FastAPI(redoc_url=None, openapi_tags=tags_metadata)
app.include_router(mmk_router, tags=["MMK"])
app.include_router(front, tags=["Site"])
app.include_router(documents, tags=["Documents"])


def task_scheduler():
    logging.info(f"task_scheduler started")
    schedule.every(10).minutes.do(schedulers.update_from_db1c)

    while True:
        schedule.run_pending()
        time.sleep(1)


@app.on_event("startup")
def startup_event():
    p = multiprocessing.Process(target=task_scheduler, daemon=True)
    p.start()
