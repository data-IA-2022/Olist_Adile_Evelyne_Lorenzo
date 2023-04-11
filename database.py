from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import yaml

def load_config(file_path):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config

config = load_config("config.yml")
