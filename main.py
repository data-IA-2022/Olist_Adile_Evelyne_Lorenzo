import asyncpg
from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import yaml
from sshtunnel import SSHTunnelForwarder

def load_config(file_path):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config

def create_ssh_tunnel(config):
    ssh_tunnel = SSHTunnelForwarder(
        (config["ssh"]["host"], 22),
        ssh_username=config["ssh"]["username"],
        ssh_password=config["ssh"]["password"],
        local_bind_address=(config["ssh"]["local_bind_address"], config["ssh"]["local_bind_port"]),
        remote_bind_address=(config["ssh"]["remote_bind_address"], config["ssh"]["remote_bind_port"]),
    )
    return ssh_tunnel

app = FastAPI()

# Configurer Jinja2
templates = Jinja2Templates(directory="templates")

# Connexion à la base de données
async def connect_to_db():
    config = load_config("config.yml")
    ssh_tunnel = create_ssh_tunnel(config)
    ssh_tunnel.start()
    conn = await asyncpg.connect(config["postgres"]["url"])
    return conn


@app.get("/", response_class=HTMLResponse)
async def root(request: Request, conn = Depends(connect_to_db)):
    query = "SELECT * FROM olist_customers_dataset LIMIT 15;"
    rows = await conn.fetch(query)
    return templates.TemplateResponse("index.html", {"request": request, "rows": rows})
