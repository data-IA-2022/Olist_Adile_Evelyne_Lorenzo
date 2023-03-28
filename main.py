import asyncpg
from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import yaml
from sshtunnel import SSHTunnelForwarder
from googletrans import Translator

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

# Ajoutez cette fonction pour traduire le texte en français
def translate_to_french(text: str) -> str:
    translator = Translator()
    translated = translator.translate(text, dest='fr')
    return translated.text

app = FastAPI()

# Configurer Jinja2
templates = Jinja2Templates(directory="templates")

async def update_translations(conn):
    try:
        print("Création de la colonne product_category_name_french...")  # Ajouter print
        create_column_query = """ALTER TABLE product_category_name_translation
                                 ADD COLUMN IF NOT EXISTS product_category_name_french TEXT;"""
        await conn.execute(create_column_query)

        print("Récupération des noms de catégorie en anglais...")  # Ajouter print
        select_query = """SELECT id, product_category_name_english
                          FROM product_category_name_translation;"""
        rows = await conn.fetch(select_query)

        print("Mise à jour des traductions en français...")  # Ajouter print
        for row in rows:
            french_translation = translate_to_french(row['product_category_name_english'])
            update_query = """UPDATE product_category_name_translation
                              SET product_category_name_french = $1
                              WHERE id = $2;"""
            await conn.execute(update_query, french_translation, row['id'])

        print("Mise à jour des traductions terminée.")  # Ajouter print
    except Exception as e:
        print(f"Erreur lors de la mise à jour des traductions : {e}")  # Ajouter print

async def connect_to_db():
    try:
        print("Chargement de la configuration...")  # Ajouter print
        config = load_config("config.yml")
        ssh_tunnel = create_ssh_tunnel(config)
        print("Démarrage du tunnel SSH...")  # Ajouter print
        ssh_tunnel.start()
        print("Connexion à la base de données...")  # Ajouter print
        conn = await asyncpg.connect(config["postgres"]["url"])
        print("Mise à jour des traductions...")  # Ajouter print
        await update_translations(conn)
        return conn
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")  # Ajouter print

@app.get("/", response_class=HTMLResponse)
async def root(request: Request, conn = Depends(connect_to_db)):
    query = "SELECT * FROM olist_customers_dataset LIMIT 15;"
    rows = await conn.fetch(query)
    return templates.TemplateResponse("index.html", {"request": request, "rows": rows})
