from datetime import date
from food_tracking import FoodTracking

def create_users() -> dict:
    """
    Create the available users in our API
    5 users, with each different food consumption habits
    Each user has a different number of foods consumed daily
    As well as different probabilities for eating specific food categories
    """

    user_name = ["Alice", "Bob", "Charlie", "David", "Eve"]
    user_avg_foods = [5, 4, 6, 3, 7]
    user_std_foods = [1, 1, 2, 1, 3]
    perc_fruit = [0.3, 0.2, 0.25, 0.2, 0.35]
    perc_vegetable = [0.4, 0.3, 0.35, 0.25, 0.4]
    perc_meat = [0.2, 0.25, 0.2, 0.35, 0.15]
    perc_dairy = [0.1, 0.25, 0.2, 0.2, 0.1]

    # Example food database with nutrition information
    food_db = {
        "fruit": {"calories": 50, "protein": 0.5, "carbs": 13, "fat": 0.2},
        "vegetable": {"calories": 30, "protein": 2, "carbs": 5, "fat": 0.1},
        "meat": {"calories": 250, "protein": 20, "carbs": 0, "fat": 15},
        "dairy": {"calories": 150, "protein": 8, "carbs": 12, "fat": 8},
    }

    user_dict = dict()

    for i in range(len(user_name)):
        user_dict[user_name[i]] = FoodTracking(
            user_name[i],
            user_avg_foods[i],
            user_std_foods[i],
            perc_fruit[i],
            perc_vegetable[i],
            perc_meat[i],
            perc_dairy[i],
            food_db
        )
    return user_dict

if __name__ == "__main__":
    users = create_users()
    for user in users.values():
        print(user.simulate_food_consumption(date(2023, 10, 25)))
