import sys
import os

# Add the exact path to 'data_engineering' to the PYTHONPATH
sys.path.insert(0, r'C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering')

# Print sys.path for debugging
print("sys.path:", sys.path)

try:
    from data_engineering.sensor_api.fake_data.sensor import create_user_instance
    print("Import de 'create_user_instance' réussi depuis 'fake_data.sensor'")
except ImportError as e:
    print(f"Échec de l'import: {e}")

try:
    from data_engineering.sensor_api.fake_data.app_tracker import AppTracker
    print("Import de 'AppTracker' réussi depuis 'fake_data.app_tracker'")
except ImportError as e:
    print(f"Échec de l'import: {e}")
