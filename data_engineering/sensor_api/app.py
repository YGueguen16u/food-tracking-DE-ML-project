from __future__ import annotations

from datetime import date

import pandas as pd

from typing import Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from fake_data import create_app

app_tracker = create_app()
user_list = app_tracker.users
app = FastAPI()


def find_user_by_id(users, user_id):
    for user in users:
        if user.user_id == user_id:
            return user
    return None


# https://food-tracking-de-ml-project.onrender.com/?user_id=4&year=2024&month=07&day=18&meal_id=1

# curl -G https://fake-retail-sensor-api.onrender.com -d "user_id=4" -d "year=2024" -d "month=07" -d "day=18"
@app.get("/")
def connexion(
        user_id: int,
        year: int,
        month: int,
        day: int,
        meal_id: Optional[int] = None,
) -> JSONResponse:
    # Trouver l'utilisateur par user_id
    user = find_user_by_id(user_list, user_id)
    if user is None:
        return JSONResponse(status_code=404, content="User Not found")

    # Check the year
    if year < 2024:
        return JSONResponse(status_code=404, content="No data before 2024")

    # Check the date
    try:
        date(year, month, day)
    except TypeError:
        return JSONResponse(status_code=404, content="Enter a valid date")

    # Check the date is in the past
    if date.today() < date(year, month, day):
        return JSONResponse(status_code=404, content="Choose a date in the past")

    # If no sensor choose return the visit for the whole store
    if meal_id is None:
        connexion_counts = app_tracker.get_all_connexion(
            user_id,
            date(year, month, day)
        )
    else:
        classe_mangeur = user.classe_mangeur
        # Check the value of meal_id
        if classe_mangeur in ['standard', 'meat_lover', 'vegan', 'vegetarian'] and (meal_id > 4 or meal_id < 1):
            return JSONResponse(status_code=404, content="Meal_id should be between 1 and 4")
        if classe_mangeur == 'random' and meal_id != 1:
            return JSONResponse(status_code=404, content="Meal_id should be 1")
        if classe_mangeur == 'fasting' and (meal_id > 2 or meal_id < 1):
            return JSONResponse(status_code=404, content="Meal_id should be between 1 and 2")

        connexion_counts = app_tracker.get_connexion(meal_id, date(year, month, day), user_id)

    #if connexion_counts < 0:
        #return JSONResponse(
         #   status_code=404, content="The store was closed try another date"
        #)

    return JSONResponse(status_code=200, content=connexion_counts)
