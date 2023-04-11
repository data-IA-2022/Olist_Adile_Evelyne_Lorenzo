import asyncpg
import folium
from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import yaml
from sshtunnel import SSHTunnelForwarder
from googletrans import Translator
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session


import models
import schemas


# from bokeh.plotting import figure, show
# from bokeh.tile_providers import get_provider, WIKIMEDIA, CARTODBPOSITRON, STAMEN_TERRAIN, STAMEN_TONER, ESRI_IMAGERY, OSM
# from bokeh.models import ColumnDataSource, HoverTool
# from bokeh.embed import file_html
# from bokeh.resources import CDN
# from pyproj import Proj, transform
# from bokeh.models.ranges import Range1d
# from bokeh.models.tiles import WMTSTileSource


def load_config(file_path):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def create_ssh_tunnel(config):
    ssh_tunnel = SSHTunnelForwarder(
        (config["ssh"]["host"], 22),
        ssh_username=config["ssh"]["username"],
        ssh_password=config["ssh"]["password"],
        local_bind_address=(
            config["ssh"]["local_bind_address"], config["ssh"]["local_bind_port"]),
        remote_bind_address=(
            config["ssh"]["remote_bind_address"], config["ssh"]["remote_bind_port"]),
    )
    return ssh_tunnel

# Ajoutez cette fonction pour traduire le texte en français


def translate_to_french(text: str) -> str:
    translator = Translator()
    translated = translator.translate(text, dest='fr')
    return translated.text


def translate_to_english(text: str) -> str:
    translator = Translator()
    translated = translator.translate(text, src='fr', dest='en')
    return translated.text


def translate_to_pt_and_en(text: str) -> tuple:
    translator = Translator()
    translation_pt = translator.translate(text, dest='pt')
    translation_en = translator.translate(text, dest='en')
    return (translation_pt.text, translation_en.text)


app = FastAPI()

# Configurer Jinja2
templates = Jinja2Templates(directory="templates")

config = load_config("config.yml")
ssh_tunnel = create_ssh_tunnel(config)
ssh_tunnel.start()


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
            french_translation = translate_to_french(
                row['product_category_name_english'])
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
        print("Démarrage du tunnel SSH...")
        print("Connexion à la base de données...")
        conn = await asyncpg.connect(config["postgres"]["url"])
        # print("Mise à jour des traductions...")
        # await update_translations(conn)
        # print("Mise à jour des coordonnées de géopoint...")
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
        print(
            f"Erreur lors de la mise à jour des coordonnées de géopoint : {e}")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request, conn=Depends(connect_to_db)):
    query = "SELECT * FROM olist_customers LIMIT 15;"
    rows = await conn.fetch(query)
    return templates.TemplateResponse("index.html", {"request": request, "rows": rows})


# def lat_lng_to_mercator(lat, lng):
#     in_proj = Proj(proj='latlong', datum='WGS84')
#     out_proj = Proj(proj='merc', datum='WGS84')
#     mercator_x, mercator_y = transform(in_proj, out_proj, lng, lat)
#     return mercator_x, mercator_y


# @app.get("/plot_map", response_class=HTMLResponse)
# async def plot_map(request: Request, conn=Depends(connect_to_db)):
#     dark_gray_base_url = "http://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{Z}/{Y}/{X}.png"
#     dark_gray_base = WMTSTileSource(url=dark_gray_base_url)
#     # Get data from the database
#     query = "SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng FROM olist_geolocation_bis LIMIT 1000;"
#     rows = await conn.fetch(query)

#     # Convert rows to a pandas DataFrame
#     df = pd.DataFrame(rows, columns=['zip_code', 'lat', 'lng'])

#     # Convert lat, lng to Mercator coordinates
#     df['mercator_x'], df['mercator_y'] = zip(
#         *df.apply(lambda row: lat_lng_to_mercator(row['lat'], row['lng']), axis=1))

#     # Define the tile provider for the map
#     # tile_provider = get_provider(STAMEN_TERRAIN)

#     # Create a new Bokeh plot with the tile provider
#     p = figure(x_axis_type="mercator", y_axis_type="mercator",
#                plot_width=1000, plot_height=1000)
#     p.add_tile(dark_gray_base)

#     # Add markers for each point with zip code information
#     source = ColumnDataSource(df)
#     hover_tool = HoverTool(tooltips=[("Zip code", "@zip_code")])
#     p.add_tools(hover_tool)
#     p.circle(x='mercator_x', y='mercator_y',
#              size=5, color='red', source=source)

#     # Get the HTML representation of the plot
#     plot_html = file_html(p, CDN, "my plot")

#     return templates.TemplateResponse("plot_map.html", {"request": request, "plot_html": plot_html})


# @app.get("/seller", response_class=HTMLResponse)
# async def seller_zip_codes(request: Request, conn=Depends(connect_to_db)):
#     dark_gray_base_url = "http://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{Z}/{Y}/{X}.png"
#     dark_gray_base = WMTSTileSource(url=dark_gray_base_url)

#     # Get data from the database
#     query = """
#     SELECT DISTINCT s.seller_zip_code_prefix, g.geolocation_lat, g.geolocation_lng
#     FROM olist_sellers s
#     JOIN olist_geolocation_bis g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix;
#     """
#     rows = await conn.fetch(query)

#     # Convert rows to a pandas DataFrame
#     df = pd.DataFrame(rows, columns=['zip_code', 'lat', 'lng'])

#     # Convert lat, lng to Mercator coordinates
#     df['mercator_x'], df['mercator_y'] = zip(
#         *df.apply(lambda row: lat_lng_to_mercator(row['lat'], row['lng']), axis=1))

#     # Define the tile provider for the map
#     p = figure(x_axis_type="mercator", y_axis_type="mercator",
#                plot_width=1000, plot_height=1000)
#     p.add_tile(dark_gray_base)

#     # Add markers for each point with zip code information
#     source = ColumnDataSource(df)
#     hover_tool = HoverTool(tooltips=[("Zip code", "@zip_code")])
#     p.add_tools(hover_tool)
#     p.circle(x='mercator_x', y='mercator_y',
#              size=8, color='blue', source=source)

#     # Get the HTML representation of the plot
#     plot_html = file_html(p, CDN, "my plot")

#     return templates.TemplateResponse("seller_zip_codes.html", {"request": request, "plot_html": plot_html})


@app.post("/add_category")
async def add_category(request: Request, product_name: schemas.Product_name, product_category_name_french: str = Form(...), conn=Depends(connect_to_db)):
    try:
        # Traduction en portugais brésilien et anglais
        translation_pt, translation_en = translate_to_pt_and_en(
            product_category_name_french)

        # Insérer la nouvelle catégorie
        # insert_query = """INSERT INTO product_category_name_translation (product_category_name, product_category_name_english, product_category_name_french)
        #                     VALUES ($1, $2, $3);"""
        new_category = models.ProductCategory(
            product_category_name=translation_pt,
            product_category_name_english=translation_en,
            product_category_name_french=product_category_name_french
        )
        # Ajouter la nouvelle catégorie à la session
        conn.add(new_category)
        # Effectuer la transaction
        conn.commit()

        return {"message": "Catégorie ajoutée avec succès"}
    except Exception as e:
        print(f"Erreur lors de l'ajout de la catégorie : {e}")
        return {"message": f"Erreur lors de l'ajout de la catégorie : {e}"}


@app.get("/translation", response_class=HTMLResponse)
async def get_translations(request: Request, conn=Depends(connect_to_db)):
    query = "SELECT * FROM product_category_name_translation ORDER BY id DESC;"

    cats = await conn.fetch(query)
    return templates.TemplateResponse("translation.html", {"request": request, "rows": cats})
