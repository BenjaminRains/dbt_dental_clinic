from dotenv import load_dotenv
import os
import pathlib

# Get the absolute path to the config directory
config_dir = pathlib.Path(__file__).parent.absolute()
env_path = os.path.join(config_dir, '.env')

# Load environment variables from the .env file in the config directory
load_dotenv(env_path)