import asyncpg
from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os

app = FastAPI()

# Configurer Jinja2
templates = Jinja2Templates(directory="templates")

# Paramètres de connexion à la base de données PostgreSQL
DATABASE_URL = "postgresql://postgres:greta2023@localhost:5432/olist_test"

# Connexion à la base de données
async def connect_to_db():
    conn = await asyncpg.connect(DATABASE_URL)
    return conn

@app.get("/", response_class=HTMLResponse)
async def root(request: Request, conn = Depends(connect_to_db)):
    query = "SELECT * FROM olist_customers_dataset"
    rows = await conn.fetch(query)
    return templates.TemplateResponse("index.html", {"request": request, "rows": rows})
