import sys
import os

# Debug prints
print("Current file:", __file__)
print("Current directory:", os.path.dirname(__file__))
print("Parent directory:", os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Add the parent directory of 'data_engineering' to the PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

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
