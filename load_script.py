import os
import pandas as pd
from sqlalchemy import create_engine

# Créer une connexion à la base de données PostgreSQL
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="olist_test",
    user="postgres",
    password="greta2023"
)

# Créer un moteur SQLAlchemy pour la connexion à la base de données PostgreSQL
engine = create_engine('postgresql+psycopg2://', creator=lambda: conn, echo=False)

# Chemin d'accès au dossier contenant les fichiers CSV
path = r'C:\Users\loren\Desktop\MOOC\MOOC_G4_Maud_Lorenzo\Olist_Adile_Evelyne_Lorenzo\archive'

# Parcourir tous les fichiers CSV dans le dossier et les insérer dans la base de données

# for filename in os.listdir(path):
#     if filename.endswith('.csv'):
#         # Charger le fichier CSV dans un dataframe pandas
#         df = pd.read_csv(os.path.join(path, filename))
#         # Supprimer les espaces de début et de fin des noms de colonnes
#         df.columns = df.columns.str.strip()
#         # Insérer les données dans la base de données SQLite
#         df.to_sql(filename[:-4], engine, if_exists='replace', index=False)

# with conn.cursor() as cursor:
#     cursor.execute("""DELETE FROM olist_order_reviews_dataset
#     WHERE review_id IN (
#         SELECT review_id
#         FROM (
#             SELECT review_id,
#                 ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_id) AS row_number
#             FROM olist_order_reviews_dataset
#         ) AS rows
#         WHERE rows.row_number > 1
#     );
#     """)
#     conn.commit()

# with conn.cursor() as cursor:
#     # Créer une table temporaire sans doublons
#     cursor.execute("""
#         DELETE FROM olist_geolocation_dataset
#     WHERE (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) IN (
#     SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
#     FROM olist_geolocation_dataset
#     GROUP BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
#     HAVING COUNT(*) > 1
# );""")
#     conn.commit()

# Ajouter des clés primaires
with conn.cursor() as cursor:
    
    cursor.execute("ALTER TABLE olist_customers_dataset ADD CONSTRAINT pk_olist_customers_dataset PRIMARY KEY (customer_id);")
    cursor.execute("ALTER TABLE olist_geolocation_dataset ADD CONSTRAINT pk_olist_geolocation_dataset PRIMARY KEY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng);")
    cursor.execute("ALTER TABLE olist_order_items_dataset ADD CONSTRAINT pk_olist_order_items_dataset PRIMARY KEY (order_id, order_item_id);")
    cursor.execute("ALTER TABLE olist_order_payments_dataset ADD CONSTRAINT pk_olist_order_payments_dataset PRIMARY KEY (order_id, payment_sequential);")
    cursor.execute("ALTER TABLE olist_order_reviews_dataset ADD CONSTRAINT pk_olist_order_reviews_dataset PRIMARY KEY (review_id);")
    cursor.execute("ALTER TABLE olist_orders_dataset ADD CONSTRAINT pk_olist_orders_dataset PRIMARY KEY (order_id);")
    cursor.execute("ALTER TABLE olist_products_dataset ADD CONSTRAINT pk_olist_products_dataset PRIMARY KEY (product_id);")
    cursor.execute("ALTER TABLE olist_sellers_dataset ADD CONSTRAINT pk_olist_sellers_dataset PRIMARY KEY (seller_id);")
    cursor.execute("ALTER TABLE product_category_name_translation ADD CONSTRAINT pk_product_category_name_translation PRIMARY KEY (product_category_name);")
    conn.commit()


# Ajouter des clés étrangères
# with conn.cursor() as cursor:
    
#     cursor.execute("""ALTER TABLE olist_customers_dataset ADD CONSTRAINT fk_customer_zip_code_prefix FOREIGN KEY (customer_zip_code_prefix) REFERENCES olist_geolocation_dataset(geolocation_zip_code_prefix, geolocation_lat, geolocation_lng);""")
#     conn.commit()

    
#     # Ajouter des contraintes pour les colonnes de type VARCHAR
#     cursor.execute("ALTER TABLE olist_customers_dataset ALTER COLUMN customer_city TYPE VARCHAR(50);")
#     cursor.execute("ALTER TABLE olist_customers_dataset ALTER COLUMN customer_state TYPE VARCHAR(2);")
#     cursor.execute("ALTER TABLE olist_geolocation_dataset ALTER COLUMN geolocation_city TYPE VARCHAR(50);")
#     cursor.execute("ALTER TABLE olist_geolocation_dataset ALTER COLUMN geolocation_state TYPE VARCHAR(2);")
#     cursor.execute("ALTER TABLE olist_order_reviews_dataset ALTER COLUMN review_comment_title TYPE VARCHAR(100);")
#     cursor.execute("ALTER TABLE olist_order_reviews_dataset ALTER COLUMN review_comment_message TYPE VARCHAR(500);")
#     cursor.execute("ALTER TABLE olist_products_dataset ALTER COLUMN product_category_name TYPE VARCHAR(50);")
#     cursor.execute("ALTER TABLE olist_products_dataset ALTER COLUMN product_name_lenght TYPE VARCHAR(50);")
#     cursor.execute("ALTER TABLE olist_products_dataset ALTER COLUMN product_description_lenght TYPE VARCHAR(50);")
#     cursor.execute("ALTER TABLE olist_products_dataset ALTER COLUMN product_photos_qty TYPE VARCHAR(50);")
#     cursor.execute("ALTER TABLE olist_sellers_dataset ALTER COLUMN seller_city TYPE VARCHAR(50);")
#     cursor.execute("ALTER TABLE olist_sellers_dataset ALTER COLUMN seller_state TYPE VARCHAR(2);")
    
#     conn.commit()

# # Fermer la connexion à la base de données
# engine.dispose()