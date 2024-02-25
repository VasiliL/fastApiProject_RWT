from routes.mmk_oracle import router as mmk_router
from routes.front_interaction import router as front
from fastapi import FastAPI


tags_metadata = [
    {
        "name": "Site",
        "description": "Operations with front-end site. The **front-end** is cool.",
    },
    {
        "name": "MMK",
        "description": "Operations with MMK Oracle subsystem."
    },
]

app = FastAPI(redoc_url=None, openapi_tags=tags_metadata)
app.include_router(mmk_router, tags=["MMK"])
app.include_router(front, tags=["Site"])
