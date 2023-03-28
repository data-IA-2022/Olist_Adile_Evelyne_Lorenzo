import os
import pandas as pd
from sqlalchemy import create_engine

# Créer une connexion à la base de données PostgreSQL
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="test",
    user="postgres",
    password="greta2023"
)

# Créer un moteur SQLAlchemy pour la connexion à la base de données PostgreSQL
engine = create_engine('postgresql+psycopg2://', creator=lambda: conn, echo=False)

# Chemin d'accès au dossier contenant les fichiers CSV
path = r'C:\Users\loren\Desktop\MOOC\MOOC_G4_Maud_Lorenzo\Olist_Adile_Evelyne_Lorenzo\archive'

# Parcourir tous les fichiers CSV dans le dossier et les insérer dans la base de données

for filename in os.listdir(path):
    if filename.endswith('.csv'):
        # Charger le fichier CSV dans un dataframe pandas
        # Spécifiez le type de données pour les colonnes customer_zip_code_prefix et geolocation_zip_code_prefix
        dtype = None
        if 'customers' in filename:
            dtype = {'customer_zip_code_prefix': str}
        elif 'geolocation' in filename:
            dtype = {'geolocation_zip_code_prefix': str}
        elif 'sellers' in filename:
            dtype = {'seller_zip_code_prefix': str}

        df = pd.read_csv(os.path.join(path, filename), dtype=dtype)
        # Supprimer les espaces de début et de fin des noms de colonnes
        df.columns = df.columns.str.strip()
        # Insérer les données dans la base de données PostgreSQL
        df.to_sql(filename[:-4], engine, if_exists='replace', index=False)
        
with conn.cursor() as cursor:
    cursor.execute("""DELETE FROM olist_order_reviews_dataset
    WHERE review_id IN (
        SELECT review_id
        FROM (
            SELECT review_id,
                ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_id) AS row_number
            FROM olist_order_reviews_dataset
        ) AS rows
        WHERE rows.row_number > 1
    );
    """)
    conn.commit()

with conn.cursor() as cursor:
    # Créer une table temporaire sans doublons
    cursor.execute("""
        DELETE FROM olist_geolocation_dataset
    WHERE (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) IN (
    SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
    FROM olist_geolocation_dataset
    GROUP BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
    HAVING COUNT(*) > 1
);""")
    conn.commit()

#################################################################################################
# Ajout des clés primaires
#################################################################################################

with conn.cursor() as cursor:
    
    cursor.execute("ALTER TABLE olist_customers_dataset ADD CONSTRAINT pk_olist_customers_dataset PRIMARY KEY (customer_id);")
    cursor.execute("ALTER TABLE olist_order_items_dataset ADD CONSTRAINT pk_olist_order_items_dataset PRIMARY KEY (order_id, order_item_id);")
    cursor.execute("ALTER TABLE olist_order_payments_dataset ADD CONSTRAINT pk_olist_order_payments_dataset PRIMARY KEY (order_id, payment_sequential);")
    cursor.execute("ALTER TABLE olist_order_reviews_dataset ADD CONSTRAINT pk_olist_order_reviews_dataset PRIMARY KEY (review_id, order_id);")
    cursor.execute("ALTER TABLE olist_orders_dataset ADD CONSTRAINT pk_olist_orders_dataset PRIMARY KEY (order_id);")
    cursor.execute("ALTER TABLE olist_products_dataset ADD CONSTRAINT pk_olist_products_dataset PRIMARY KEY (product_id);")
    cursor.execute("ALTER TABLE olist_sellers_dataset ADD CONSTRAINT pk_olist_sellers_dataset PRIMARY KEY (seller_id);")
    cursor.execute("ALTER TABLE product_category_name_translation ADD CONSTRAINT pk_product_category_name_translation PRIMARY KEY (product_category_name);")
    conn.commit()

#################################################################################################
# Ajout des contraintes pour les colonnes de type VARCHAR
#################################################################################################

with conn.cursor() as cursor:
    cursor.execute("ALTER TABLE olist_customers_dataset ALTER COLUMN customer_city TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_customers_dataset ALTER COLUMN customer_state TYPE VARCHAR(2);")
    # cursor.execute("ALTER TABLE olist_customers_dataset ALTER COLUMN customer_zip_code_prefix TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_customers_dataset ALTER COLUMN customer_id TYPE VARCHAR(100);")
    cursor.execute("ALTER TABLE olist_customers_dataset ALTER COLUMN customer_unique_id TYPE VARCHAR(100);")

    cursor.execute("""ALTER TABLE public.olist_order_items_dataset ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;""")
    cursor.execute("""ALTER TABLE public.olist_order_items_dataset ALTER COLUMN product_id TYPE varchar(32) USING product_id::varchar;""")
    cursor.execute("""ALTER TABLE public.olist_order_items_dataset ALTER COLUMN seller_id TYPE varchar(32) USING seller_id::varchar;""")
    cursor.execute("""ALTER TABLE public.olist_order_items_dataset ALTER COLUMN shipping_limit_date TYPE timestamp USING shipping_limit_date::timestamp;""")
    
    cursor.execute("""ALTER TABLE public.olist_order_payments_dataset ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;""")
    cursor.execute("""ALTER TABLE public.olist_order_payments_dataset ALTER COLUMN payment_type TYPE varchar(32) USING payment_type::varchar;""")


    cursor.execute("ALTER TABLE olist_geolocation_dataset ALTER COLUMN geolocation_city TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_geolocation_dataset ALTER COLUMN geolocation_state TYPE VARCHAR(2);")

    
    cursor.execute("ALTER TABLE public.olist_order_reviews_dataset ALTER COLUMN review_id TYPE varchar(32) USING review_id::varchar;")
    cursor.execute("ALTER TABLE public.olist_order_reviews_dataset ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;")
    cursor.execute("ALTER TABLE olist_order_reviews_dataset ALTER COLUMN review_comment_title TYPE VARCHAR(100);")
    cursor.execute("ALTER TABLE olist_order_reviews_dataset ALTER COLUMN review_comment_message TYPE VARCHAR(500);")
    cursor.execute("ALTER TABLE public.olist_order_reviews_dataset ALTER COLUMN review_creation_date TYPE timestamp USING review_creation_date::timestamp;")
    cursor.execute("ALTER TABLE public.olist_order_reviews_dataset ALTER COLUMN review_answer_timestamp TYPE timestamp USING review_answer_timestamp::timestamp;")
    
    cursor.execute("ALTER TABLE public.olist_orders_dataset ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;")
    cursor.execute("ALTER TABLE public.olist_orders_dataset ALTER COLUMN customer_id TYPE varchar(32) USING customer_id::varchar;")
    cursor.execute("ALTER TABLE public.olist_orders_dataset ALTER COLUMN order_status TYPE varchar(32) USING order_status::varchar;")
    cursor.execute("ALTER TABLE public.olist_orders_dataset ALTER COLUMN order_purchase_timestamp TYPE timestamp USING order_purchase_timestamp::timestamp;")
    cursor.execute("ALTER TABLE public.olist_orders_dataset ALTER COLUMN order_approved_at TYPE timestamp USING order_approved_at::timestamp;")
    cursor.execute("ALTER TABLE public.olist_orders_dataset ALTER COLUMN order_delivered_carrier_date TYPE timestamp USING order_delivered_carrier_date::timestamp;")
    cursor.execute("ALTER TABLE public.olist_orders_dataset ALTER COLUMN order_delivered_customer_date TYPE timestamp USING order_delivered_customer_date::timestamp;")
    cursor.execute("ALTER TABLE public.olist_orders_dataset ALTER COLUMN order_estimated_delivery_date TYPE timestamp USING order_estimated_delivery_date::timestamp;")

    cursor.execute("ALTER TABLE public.olist_products_dataset ALTER COLUMN product_id TYPE varchar(32) USING product_id::varchar;")
    cursor.execute("ALTER TABLE olist_products_dataset ALTER COLUMN product_category_name TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_products_dataset ALTER COLUMN product_name_lenght TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_products_dataset ALTER COLUMN product_description_lenght TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_products_dataset ALTER COLUMN product_photos_qty TYPE VARCHAR(50);")

    cursor.execute("ALTER TABLE public.olist_sellers_dataset ALTER COLUMN seller_id TYPE varchar(32) USING seller_id::varchar;")
    cursor.execute("ALTER TABLE olist_sellers_dataset ALTER COLUMN seller_city TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_sellers_dataset ALTER COLUMN seller_state TYPE VARCHAR(2);")
    
    cursor.execute("ALTER TABLE public.product_category_name_translation ALTER COLUMN product_category_name TYPE varchar(100) USING product_category_name::varchar;")
    cursor.execute("ALTER TABLE public.product_category_name_translation ALTER COLUMN product_category_name_english TYPE varchar(100) USING product_category_name_english::varchar;")
    
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""CREATE TABLE public.olist_geolocation_dataset_bis (
	geolocation_zip_code_prefix varchar(32) NULL,
	geolocation_lat varchar(32) NULL,
	geolocation_lng varchar(32) NULL,
	geolocation_city varchar(50) NULL,
	geolocation_state varchar(2) NULL,
    n int NULL
);
""")
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""INSERT INTO olist_geolocation_dataset_bis
                    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state, n)
                    SELECT G.geolocation_zip_code_prefix, avg(G.geolocation_lat), avg(G.geolocation_lng),
                    MAX(G.geolocation_city), MAX(G.geolocation_state), COUNT(*)
                    FROM olist_geolocation_dataset G
                    GROUP BY G.geolocation_zip_code_prefix;""")
    conn.commit()


with conn.cursor() as cursor:
    cursor.execute("""ALTER TABLE public.olist_geolocation_dataset_bis ADD CONSTRAINT olist_geolocation_dataset_bis_pk PRIMARY KEY (geolocation_zip_code_prefix);""")
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""INSERT INTO public.olist_geolocation_dataset_bis (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state, n)
                    SELECT DISTINCT S.customer_zip_code_prefix, NULL, NULL, NULL, NULL, 0
                    FROM olist_customers_dataset S
                    LEFT JOIN public.olist_geolocation_dataset_bis G
                    ON S.customer_zip_code_prefix = G.geolocation_zip_code_prefix
                    WHERE G.geolocation_zip_code_prefix IS NULL;
                    """)
    conn.commit()

##################################################################################################
# Ajout des clés étrangères
##################################################################################################

# with conn.cursor() as cursor:
    
#     cursor.execute("""ALTER TABLE olist_customers_dataset ADD CONSTRAINT fk_customer_zip_code_prefix FOREIGN KEY (customer_zip_code_prefix) REFERENCES olist_geolocation_dataset(geolocation_zip_code_prefix, geolocation_lat, geolocation_lng);""")
#     conn.commit()

# with conn.cursor() as cursor:
#     cursor.execute("""ALTER TABLE olist_customers_dataset ADD CONSTRAINT fk_customer_zip_code_prefix FOREIGN KEY (customer_zip_code_prefix) REFERENCES olist_geolocation_dataset(geolocation_zip_code_prefix);""")
#     cursor.execute("""ALTER TABLE olist_orders_dataset ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES olist_customers_dataset(customer_id);""")
#     cursor.execute("""ALTER TABLE olist_orders_dataset ADD CONSTRAINT fk_order_items_order_id FOREIGN KEY (order_id) REFERENCES olist_order_items_dataset(order_id);""")
#     cursor.execute("""ALTER TABLE olist_order_items_dataset ADD CONSTRAINT fk_order_items_product_id FOREIGN KEY (product_id) REFERENCES olist_products_dataset(product_id);""")
#     cursor.execute("""ALTER TABLE olist_order_items_dataset ADD CONSTRAINT fk_order_items_seller_id FOREIGN KEY (seller_id) REFERENCES olist_sellers_dataset(seller_id);""")
#     cursor.execute("""ALTER TABLE olist_order_payments_dataset ADD CONSTRAINT fk_payments_order_id FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id);""")
#     cursor.execute("""ALTER TABLE olist_order_reviews_dataset ADD CONSTRAINT fk_reviews_order_id FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id);""")
#     cursor.execute("""ALTER TABLE olist_order_reviews_dataset ADD CONSTRAINT fk_reviews_product_id FOREIGN KEY (product_id) REFERENCES olist_products_dataset(product_id);""")
#     cursor.execute("""ALTER TABLE olist_sellers_dataset ADD CONSTRAINT fk_seller_zip_code_prefix FOREIGN KEY (seller_zip_code_prefix) REFERENCES olist_geolocation_dataset(geolocation_zip_code_prefix);""")
#     conn.commit()

##################################################################################################
# Table jointure
##################################################################################################

# with conn.cursor() as cursor:
#     cursor.execute("""-- création de la table "customer_geolocation"
# CREATE TABLE customer_geolocation (
#     id SERIAL PRIMARY KEY,
#     customer_id VARCHAR(255) NOT NULL,
#     customer_zip_code_prefix VARCHAR(50) NOT NULL,
#     geolocation_id INTEGER NOT NULL
# );

# """)
#     conn.commit()

# with conn.cursor() as cursor:
#     cursor.execute("""
# ALTER TABLE olist_geolocation_dataset
# ADD COLUMN id SERIAL PRIMARY KEY;

# """)
#     conn.commit()
    
# with conn.cursor() as cursor:
#     cursor.execute("""
# ALTER TABLE customer_geolocation
# ADD CONSTRAINT customer_fk FOREIGN KEY (customer_id) REFERENCES olist_customers_dataset(customer_id);

# ALTER TABLE customer_geolocation
# ADD CONSTRAINT geolocation_fk FOREIGN KEY (geolocation_id) REFERENCES olist_geolocation_dataset(id);

# """)
#     conn.commit()

# with conn.cursor() as cursor:
#     cursor.execute("""
# INSERT INTO customer_geolocation (customer_id, customer_zip_code_prefix, geolocation_id)
# SELECT c.customer_id, c.customer_zip_code_prefix, g.id
# FROM olist_customers_dataset c
# JOIN olist_geolocation_dataset g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix;

# """)
#     conn.commit()

# # Fermer la connexion à la base de données
# engine.dispose()