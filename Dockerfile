FROM python:latest
LABEL authors="VL"


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]