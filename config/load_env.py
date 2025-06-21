from dotenv import load_dotenv
import os
import pathlib

# Get the absolute path to the project root (parent of config directory)
project_root = pathlib.Path(__file__).parent.parent.absolute()
env_path = os.path.join(project_root, '.env')

# Load environment variables from the .env file in the project root
load_dotenv(env_path) 