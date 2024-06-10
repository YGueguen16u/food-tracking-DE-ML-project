from datetime import date
from data_engineering.sensor_api.fake_data.sensor import create_user_instance
import sys
import os

# Ajoutez le répertoire parent de 'data_engineering' au PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

class AppTracker:
    def __init__(self, user_data: list) -> None:
        """
        Initialize the AppTracker with user data
        """
        self.users = [create_user_instance(user) for user in user_data]

    def simulate_day(self, business_date: date, aliments_df: str) -> None:
        """
        Simulate the connections and food consumption for each user for a day
        """
        for user in self.users:
            user.simulate_daily_activity(business_date, aliments_df)

    def get_user_activity(self, user_id: int, business_date: date) -> dict:
        """
        Get the activity log for a specific user on a specific date
        """
        user = next((u for u in self.users if u.user_id == user_id), None)
        if user:
            return user.get_daily_activity(business_date)
        return {}
