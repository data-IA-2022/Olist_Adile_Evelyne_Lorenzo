BEGIN;

-- Suppression des doublons
DELETE FROM olist_order_reviews
WHERE review_id IN (
    SELECT review_id
    FROM (
        SELECT review_id,
            ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_id) AS row_number
        FROM olist_order_reviews
    ) AS rows
    WHERE rows.row_number > 1
);

DELETE FROM olist_geolocation
WHERE (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) IN (
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
FROM olist_geolocation
GROUP BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
HAVING COUNT(*) > 1
);

-- Ajout des clés primaires
ALTER TABLE olist_customers ADD CONSTRAINT pk_olist_customers PRIMARY KEY (customer_id);
ALTER TABLE olist_order_items ADD CONSTRAINT pk_olist_order_items PRIMARY KEY (order_id, order_item_id);
ALTER TABLE olist_order_payments ADD CONSTRAINT pk_olist_order_payments PRIMARY KEY (order_id, payment_sequential);
ALTER TABLE olist_order_reviews ADD CONSTRAINT pk_olist_order_reviews PRIMARY KEY (review_id, order_id);
ALTER TABLE olist_orders ADD CONSTRAINT pk_olist_orders PRIMARY KEY (order_id);
ALTER TABLE olist_products ADD CONSTRAINT pk_olist_products PRIMARY KEY (product_id);
ALTER TABLE olist_sellers ADD CONSTRAINT pk_olist_sellers PRIMARY KEY (seller_id);
ALTER TABLE product_category_name_translation ADD CONSTRAINT pk_product_category_name_translation PRIMARY KEY (product_category_name);

-- Ajout des contraintes pour les colonnes de type VARCHAR
ALTER TABLE olist_customers ALTER COLUMN customer_city TYPE VARCHAR(50);
ALTER TABLE olist_customers ALTER COLUMN customer_state TYPE VARCHAR(2);
ALTER TABLE olist_customers ALTER COLUMN customer_id TYPE VARCHAR(100);
ALTER TABLE olist_customers ALTER COLUMN customer_unique_id TYPE VARCHAR(100);
ALTER TABLE public.olist_order_items ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;
ALTER TABLE public.olist_order_items ALTER COLUMN product_id TYPE varchar(32) USING product_id::varchar;
ALTER TABLE public.olist_order_items ALTER COLUMN seller_id TYPE varchar(32) USING seller_id::varchar;
ALTER TABLE public.olist_order_items ALTER COLUMN shipping_limit_date TYPE timestamp USING shipping_limit_date::timestamp;
ALTER TABLE public.olist_order_payments ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;
ALTER TABLE public.olist_order_payments ALTER COLUMN payment_type TYPE varchar(32) USING payment_type::varchar;
BEGIN;

-- Modify columns in tables
ALTER TABLE olist_geolocation ALTER COLUMN geolocation_city TYPE VARCHAR(50);
ALTER TABLE olist_geolocation ALTER COLUMN geolocation_state TYPE VARCHAR(2);

ALTER TABLE public.olist_order_reviews ALTER COLUMN review_id TYPE varchar(32) USING review_id::varchar;
ALTER TABLE public.olist_order_reviews ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;
ALTER TABLE olist_order_reviews ALTER COLUMN review_comment_title TYPE VARCHAR(100);
ALTER TABLE olist_order_reviews ALTER COLUMN review_comment_message TYPE VARCHAR(500);
ALTER TABLE public.olist_order_reviews ALTER COLUMN review_creation_date TYPE timestamp USING review_creation_date::timestamp;
ALTER TABLE public.olist_order_reviews ALTER COLUMN review_answer_timestamp TYPE timestamp USING review_answer_timestamp::timestamp;

ALTER TABLE public.olist_orders ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;
ALTER TABLE public.olist_orders ALTER COLUMN customer_id TYPE varchar(32) USING customer_id::varchar;
ALTER TABLE public.olist_orders ALTER COLUMN order_status TYPE varchar(32) USING order_status::varchar;
ALTER TABLE public.olist_orders ALTER COLUMN order_purchase_timestamp TYPE timestamp USING order_purchase_timestamp::timestamp;
ALTER TABLE public.olist_orders ALTER COLUMN order_approved_at TYPE timestamp USING order_approved_at::timestamp;
ALTER TABLE public.olist_orders ALTER COLUMN order_delivered_carrier_date TYPE timestamp USING order_delivered_carrier_date::timestamp;
ALTER TABLE public.olist_orders ALTER COLUMN order_delivered_customer_date TYPE timestamp USING order_delivered_customer_date::timestamp;
ALTER TABLE public.olist_orders ALTER COLUMN order_estimated_delivery_date TYPE timestamp USING order_estimated_delivery_date::timestamp;

ALTER TABLE public.olist_products ALTER COLUMN product_id TYPE varchar(32) USING product_id::varchar;
ALTER TABLE olist_products ALTER COLUMN product_category_name TYPE VARCHAR(50);
ALTER TABLE olist_products ALTER COLUMN product_name_lenght TYPE VARCHAR(50);
ALTER TABLE olist_products ALTER COLUMN product_description_lenght TYPE VARCHAR(50);
ALTER TABLE olist_products ALTER COLUMN product_photos_qty TYPE VARCHAR(50);

ALTER TABLE public.olist_sellers ALTER COLUMN seller_id TYPE varchar(32) USING seller_id::varchar;
ALTER TABLE olist_sellers ALTER COLUMN seller_city TYPE VARCHAR(50);
ALTER TABLE olist_sellers ALTER COLUMN seller_state TYPE VARCHAR(2);

ALTER TABLE public.product_category_name_translation ALTER COLUMN product_category_name TYPE varchar(100) USING product_category_name::varchar;
ALTER TABLE public.product_category_name_translation ALTER COLUMN product_category_name_english TYPE varchar(100) USING product_category_name_english::varchar;

-- Create and populate table
CREATE TABLE public.olist_geolocation_bis (
    geolocation_zip_code_prefix varchar(32) NULL,
    geolocation_lat float NULL,
    geolocation_lng float NULL,
    geolocation_city varchar(50) NULL,
    geolocation_state varchar(2) NULL,
    n int NULL
);

INSERT INTO olist_geolocation_bis
    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state, n)
    SELECT G.geolocation_zip_code_prefix, avg(G.geolocation_lat), avg(G.geolocation_lng),
    MAX(G.geolocation_city), MAX(G.geolocation_state), COUNT(*)
    FROM olist_geolocation G
    GROUP BY G.geolocation_zip_code_prefix;

-- Add primary key to table
ALTER TABLE public.olist_geolocation_bis ADD CONSTRAINT olist_geolocation_bis_pk PRIMARY KEY (geolocation_zip_code_prefix);

-- Add missing zip codes to table
INSERT INTO public.olist_geolocation_bis (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state,n)
SELECT DISTINCT S.customer_zip_code_prefix, CAST(NULL AS double precision), CAST(NULL AS double precision), NULL, NULL, 0
FROM olist_customers S
LEFT JOIN public.olist_geolocation_bis G
ON S.customer_zip_code_prefix = G.geolocation_zip_code_prefix
WHERE G.geolocation_zip_code_prefix IS NULL
ON CONFLICT (geolocation_zip_code_prefix) DO NOTHING;

INSERT INTO public.olist_geolocation_bis (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state, n)
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

-- Add foreign key constraints
ALTER TABLE public.olist_sellers ADD CONSTRAINT olist_sellers_fk FOREIGN KEY (seller_zip_code_prefix) REFERENCES public.olist_geolocation_bis(geolocation_zip_code_prefix);
ALTER TABLE public.olist_customers ADD CONSTRAINT olist_customers_fk FOREIGN KEY (customer_zip_code_prefix) REFERENCES public.olist_geolocation_bis(geolocation_zip_code_prefix);
ALTER TABLE public.olist_order_items ADD CONSTRAINT olist_order_items_fk FOREIGN KEY (seller_id) REFERENCES public.olist_sellers(seller_id);
ALTER TABLE public.olist_orders ADD CONSTRAINT olist_orders_fk FOREIGN KEY (customer_id) REFERENCES public.olist_customers(customer_id);
ALTER TABLE public.olist_order_items ADD CONSTRAINT olist_order_items_fk2 FOREIGN KEY (product_id) REFERENCES public.olist_products(product_id);
ALTER TABLE public.olist_order_payments ADD CONSTRAINT olist_order_payments_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);
ALTER TABLE public.olist_order_reviews ADD CONSTRAINT olist_order_reviews_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);
ALTER TABLE public.olist_order_items ADD CONSTRAINT olist_order_items_fk3 FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);

-- Create views
CREATE VIEW payments_orders as
(SELECT A.order_id, payment_value, customer_id, order_purchase_timestamp
FROM olist_order_payments A INNER JOIN olist_orders B ON (A.order_id=B.order_id));

CREATE VIEW customers_brazil as
(SELECT customer_id, customer_zip_code_prefix, customer_city,
customer_state, region FROM olist_customers A INNER JOIN brazil_states B ON (A.customer_state=B.abbreviation));

CREATE VIEW payments_orders_customers as
(SELECT order_id, payment_value, A.customer_id, order_purchase_timestamp,
customer_zip_code_prefix, customer_city, customer_state, region FROM payments_orders A INNER JOIN customers_brazil B ON (A.customer_id=B.customer_id));

COMMIT;
