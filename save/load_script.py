import os
import pandas as pd
from sqlalchemy import create_engine

# Créer une connexion à la base de données PostgreSQL
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="t",
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
        # Enlever le mot "dataset" à la fin du nom de la table
        table_name = filename[:-4].replace("_dataset", "")
        # Insérer les données dans la base de données PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
with conn.cursor() as cursor:
    cursor.execute("""DELETE FROM olist_order_reviews
    WHERE review_id IN (
        SELECT review_id
        FROM (
            SELECT review_id,
                ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_id) AS row_number
            FROM olist_order_reviews
        ) AS rows
        WHERE rows.row_number > 1
    );
    """)
    conn.commit()

with conn.cursor() as cursor:
    # Créer une table temporaire sans doublons
    cursor.execute("""
        DELETE FROM olist_geolocation
    WHERE (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) IN (
    SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
    FROM olist_geolocation
    GROUP BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
    HAVING COUNT(*) > 1
);""")
    conn.commit()

#################################################################################################
# Ajout des clés primaires
#################################################################################################

with conn.cursor() as cursor:
    
    cursor.execute("ALTER TABLE olist_customers ADD CONSTRAINT pk_olist_customers PRIMARY KEY (customer_id);")
    cursor.execute("ALTER TABLE olist_order_items ADD CONSTRAINT pk_olist_order_items PRIMARY KEY (order_id, order_item_id);")
    cursor.execute("ALTER TABLE olist_order_payments ADD CONSTRAINT pk_olist_order_payments PRIMARY KEY (order_id, payment_sequential);")
    cursor.execute("ALTER TABLE olist_order_reviews ADD CONSTRAINT pk_olist_order_reviews PRIMARY KEY (review_id, order_id);")
    cursor.execute("ALTER TABLE olist_orders ADD CONSTRAINT pk_olist_orders PRIMARY KEY (order_id);")
    cursor.execute("ALTER TABLE olist_products ADD CONSTRAINT pk_olist_products PRIMARY KEY (product_id);")
    cursor.execute("ALTER TABLE olist_sellers ADD CONSTRAINT pk_olist_sellers PRIMARY KEY (seller_id);")
    cursor.execute("ALTER TABLE product_category_name_translation ADD CONSTRAINT pk_product_category_name_translation PRIMARY KEY (product_category_name);")
    conn.commit()

################################################################################################
# Ajout des contraintes pour les colonnes de type VARCHAR
################################################################################################

with conn.cursor() as cursor:
    cursor.execute("ALTER TABLE olist_customers ALTER COLUMN customer_city TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_customers ALTER COLUMN customer_state TYPE VARCHAR(2);")
    # cursor.execute("ALTER TABLE olist_customers_dataset ALTER COLUMN customer_zip_code_prefix TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_customers ALTER COLUMN customer_id TYPE VARCHAR(100);")
    cursor.execute("ALTER TABLE olist_customers ALTER COLUMN customer_unique_id TYPE VARCHAR(100);")

    cursor.execute("""ALTER TABLE public.olist_order_items ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;""")
    cursor.execute("""ALTER TABLE public.olist_order_items ALTER COLUMN product_id TYPE varchar(32) USING product_id::varchar;""")
    cursor.execute("""ALTER TABLE public.olist_order_items ALTER COLUMN seller_id TYPE varchar(32) USING seller_id::varchar;""")
    cursor.execute("""ALTER TABLE public.olist_order_items ALTER COLUMN shipping_limit_date TYPE timestamp USING shipping_limit_date::timestamp;""")
    
    cursor.execute("""ALTER TABLE public.olist_order_payments ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;""")
    cursor.execute("""ALTER TABLE public.olist_order_payments ALTER COLUMN payment_type TYPE varchar(32) USING payment_type::varchar;""")


    cursor.execute("ALTER TABLE olist_geolocation ALTER COLUMN geolocation_city TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_geolocation ALTER COLUMN geolocation_state TYPE VARCHAR(2);")

    
    cursor.execute("ALTER TABLE public.olist_order_reviews ALTER COLUMN review_id TYPE varchar(32) USING review_id::varchar;")
    cursor.execute("ALTER TABLE public.olist_order_reviews ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;")
    cursor.execute("ALTER TABLE olist_order_reviews ALTER COLUMN review_comment_title TYPE VARCHAR(100);")
    cursor.execute("ALTER TABLE olist_order_reviews ALTER COLUMN review_comment_message TYPE VARCHAR(500);")
    cursor.execute("ALTER TABLE public.olist_order_reviews ALTER COLUMN review_creation_date TYPE timestamp USING review_creation_date::timestamp;")
    cursor.execute("ALTER TABLE public.olist_order_reviews ALTER COLUMN review_answer_timestamp TYPE timestamp USING review_answer_timestamp::timestamp;")
    
    cursor.execute("ALTER TABLE public.olist_orders ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;")
    cursor.execute("ALTER TABLE public.olist_orders ALTER COLUMN customer_id TYPE varchar(32) USING customer_id::varchar;")
    cursor.execute("ALTER TABLE public.olist_orders ALTER COLUMN order_status TYPE varchar(32) USING order_status::varchar;")
    cursor.execute("ALTER TABLE public.olist_orders ALTER COLUMN order_purchase_timestamp TYPE timestamp USING order_purchase_timestamp::timestamp;")
    cursor.execute("ALTER TABLE public.olist_orders ALTER COLUMN order_approved_at TYPE timestamp USING order_approved_at::timestamp;")
    cursor.execute("ALTER TABLE public.olist_orders ALTER COLUMN order_delivered_carrier_date TYPE timestamp USING order_delivered_carrier_date::timestamp;")
    cursor.execute("ALTER TABLE public.olist_orders ALTER COLUMN order_delivered_customer_date TYPE timestamp USING order_delivered_customer_date::timestamp;")
    cursor.execute("ALTER TABLE public.olist_orders ALTER COLUMN order_estimated_delivery_date TYPE timestamp USING order_estimated_delivery_date::timestamp;")

    cursor.execute("ALTER TABLE public.olist_products ALTER COLUMN product_id TYPE varchar(32) USING product_id::varchar;")
    cursor.execute("ALTER TABLE olist_products ALTER COLUMN product_category_name TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_products ALTER COLUMN product_name_lenght TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_products ALTER COLUMN product_description_lenght TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_products ALTER COLUMN product_photos_qty TYPE VARCHAR(50);")

    cursor.execute("ALTER TABLE public.olist_sellers ALTER COLUMN seller_id TYPE varchar(32) USING seller_id::varchar;")
    cursor.execute("ALTER TABLE olist_sellers ALTER COLUMN seller_city TYPE VARCHAR(50);")
    cursor.execute("ALTER TABLE olist_sellers ALTER COLUMN seller_state TYPE VARCHAR(2);")
    
    cursor.execute("ALTER TABLE public.product_category_name_translation ALTER COLUMN product_category_name TYPE varchar(100) USING product_category_name::varchar;")
    cursor.execute("ALTER TABLE public.product_category_name_translation ALTER COLUMN product_category_name_english TYPE varchar(100) USING product_category_name_english::varchar;")
    
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""CREATE TABLE public.olist_geolocation_bis (
	geolocation_zip_code_prefix varchar(32) NULL,
	geolocation_lat float NULL,
	geolocation_lng float NULL,
	geolocation_city varchar(50) NULL,
	geolocation_state varchar(2) NULL,
    n int NULL
);
""")
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""INSERT INTO olist_geolocation_bis
                    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state, n)
                    SELECT G.geolocation_zip_code_prefix, avg(G.geolocation_lat), avg(G.geolocation_lng),
                    MAX(G.geolocation_city), MAX(G.geolocation_state), COUNT(*)
                    FROM olist_geolocation G
                    GROUP BY G.geolocation_zip_code_prefix;""")
    conn.commit()


with conn.cursor() as cursor:
    cursor.execute("""ALTER TABLE public.olist_geolocation_bis ADD CONSTRAINT olist_geolocation_bis_pk PRIMARY KEY (geolocation_zip_code_prefix);""")
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""
        INSERT INTO public.olist_geolocation_bis (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state, n)
        SELECT DISTINCT S.customer_zip_code_prefix, CAST(NULL AS double precision), CAST(NULL AS double precision), NULL, NULL, 0
        FROM olist_customers S
        LEFT JOIN public.olist_geolocation_bis G
        ON S.customer_zip_code_prefix = G.geolocation_zip_code_prefix
        WHERE G.geolocation_zip_code_prefix IS NULL
        ON CONFLICT (geolocation_zip_code_prefix) DO NOTHING;
    """)
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""INSERT INTO public.olist_geolocation_bis (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state, n)
                    SELECT S.seller_zip_code_prefix, CAST(NULL AS double precision), CAST(NULL AS double precision), NULL, NULL, 0
                    FROM olist_sellers S
                    LEFT JOIN public.olist_geolocation_bis G
                    ON S.seller_zip_code_prefix = G.geolocation_zip_code_prefix
                    WHERE G.geolocation_zip_code_prefix IS NULL
                    ON CONFLICT (geolocation_zip_code_prefix) DO UPDATE
                    SET geolocation_lat = EXCLUDED.geolocation_lat,
                        geolocation_lng = EXCLUDED.geolocation_lng,
                        geolocation_city = EXCLUDED.geolocation_city,
                        geolocation_state = EXCLUDED.geolocation_state,
                        n = EXCLUDED.n;
                    """)
    conn.commit()

    
#################################################################################################
# Ajout des clés étrangères
#################################################################################################

with conn.cursor() as cursor:
    
    cursor.execute("""ALTER TABLE public.olist_sellers ADD CONSTRAINT olist_sellers_fk FOREIGN KEY (seller_zip_code_prefix) REFERENCES public.olist_geolocation_bis(geolocation_zip_code_prefix);""")
    cursor.execute("""ALTER TABLE public.olist_customers ADD CONSTRAINT olist_customers_fk FOREIGN KEY (customer_zip_code_prefix) REFERENCES public.olist_geolocation_bis(geolocation_zip_code_prefix);""")
    cursor.execute("""ALTER TABLE public.olist_order_items ADD CONSTRAINT olist_order_items_fk FOREIGN KEY (seller_id) REFERENCES public.olist_sellers(seller_id);""")
    cursor.execute("""ALTER TABLE public.olist_orders ADD CONSTRAINT olist_orders_fk FOREIGN KEY (customer_id) REFERENCES public.olist_customers(customer_id);""")
    cursor.execute("""ALTER TABLE public.olist_order_items ADD CONSTRAINT olist_order_items_fk2 FOREIGN KEY (product_id) REFERENCES public.olist_products(product_id);""")
    cursor.execute("""ALTER TABLE public.olist_order_payments ADD CONSTRAINT olist_order_payments_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);""")
    cursor.execute("""ALTER TABLE public.olist_order_reviews ADD CONSTRAINT olist_order_reviews_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);""")
    cursor.execute("""ALTER TABLE public.olist_order_items ADD CONSTRAINT olist_order_items_fk3 FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);""")
    # cursor.execute("""ALTER TABLE public.olist_products_dataset ADD CONSTRAINT olist_products_dataset_fk FOREIGN KEY (product_category_name) REFERENCES public.product_category_name_translation(product_category_name);""")
    
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""CREATE VIEW payments_orders as
(SELECT A.order_id, payment_value, customer_id, order_purchase_timestamp
FROM olist_order_payments A INNER JOIN olist_orders B ON (A.order_id=B.order_id)) ;""")
    conn.commit()
    
with conn.cursor() as cursor:
    cursor.execute("""CREATE VIEW customers_brazil as
(SELECT customer_id, customer_zip_code_prefix, customer_city,
customer_state, region FROM olist_customers A INNER JOIN brazil_states B ON (A.customer_state=B.abbreviation));""")
    conn.commit()
    
with conn.cursor() as cursor:
    cursor.execute("""CREATE VIEW payments_orders_customers as
(SELECT order_id, payment_value, A.customer_id, order_purchase_timestamp,
customer_zip_code_prefix, customer_city, customer_state, region FROM payments_orders A INNER JOIN customers_brazil B ON (A.customer_id=B.customer_id));""")
    conn.commit()
    
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