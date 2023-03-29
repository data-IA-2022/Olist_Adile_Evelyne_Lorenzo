import asyncpg
from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import yaml
from sshtunnel import SSHTunnelForwarder
from googletrans import Translator
from psycopg2.extras import execute_values

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
        print("Création de la colonne product_category_name_french...")
        create_column_query = """ALTER TABLE product_category_name_translation
                                ADD COLUMN IF NOT EXISTS product_category_name_french TEXT;"""
        await conn.execute(create_column_query)

        print("Récupération des noms de catégorie en anglais...")
        select_query = """SELECT product_category_name, product_category_name_english
                        FROM product_category_name_translation;"""
        rows = await conn.fetch(select_query)

        print("Mise à jour des traductions en français...")
        for row in rows:
            french_translation = translate_to_french(row['product_category_name_english'])
            update_query = """UPDATE product_category_name_translation
                            SET product_category_name_french = $1
                            WHERE product_category_name = $2;"""
            await conn.execute(update_query, french_translation, row['product_category_name'])

        print("Mise à jour des traductions terminée.")
    except Exception as e:
        print(f"Erreur lors de la mise à jour des traductions : {e}")
        
async def connect_to_db():
    try:
        print("Chargement de la configuration...")
        config = load_config("config.yml")
        ssh_tunnel = create_ssh_tunnel(config)
        print("Démarrage du tunnel SSH...")
        ssh_tunnel.start()
        print("Connexion à la base de données...")
        conn = await asyncpg.connect(config["postgres"]["url"])
        print("Mise à jour des traductions...")
        # await update_translations(conn)
        print("Mise à jour des coordonnées de géopoint...")
        # await update_geolocation(conn)  # Ajouter cet appel
        return conn
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")

        
async def update_geolocation(conn):
    try:
        print("Création de la colonne geopoint_location...")
        create_column_query = """ALTER TABLE olist_geolocation_bis
                                    ADD COLUMN IF NOT EXISTS geopoint_location POINT;"""
        await conn.execute(create_column_query)

        print("Récupération des latitudes et longitudes...")
        select_query = """SELECT geolocation_lat, geolocation_lng
                        FROM olist_geolocation_bis;"""
        rows = await conn.fetch(select_query)

        print("Mise à jour des coordonnées de géopoint...")
        for row in rows:
            latitude = row['geolocation_lat']
            longitude = row['geolocation_lng']
            if latitude and longitude:
                update_query = """UPDATE olist_geolocation_bis
                                SET geopoint_location = POINT($1, $2)
                                WHERE geolocation_lat = $3
                                AND geolocation_lng = $4;"""
                await conn.execute(update_query, longitude, latitude, latitude, longitude)

        print("La colonne geopoint_location a été ajoutée avec succès.")
    except Exception as e:
        print(f"Erreur lors de la mise à jour des coordonnées de géopoint : {e}")

@app.get("/", response_class=HTMLResponse)
async def root(request: Request, conn = Depends(connect_to_db)):
    query = "SELECT * FROM olist_customers LIMIT 15;"
    rows = await conn.fetch(query)
    return templates.TemplateResponse("index.html", {"request": request, "rows": rows})
