from datetime import date

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from fake_data import create_app

store_dict = create_app()
app = FastAPI()


@app.get("/")