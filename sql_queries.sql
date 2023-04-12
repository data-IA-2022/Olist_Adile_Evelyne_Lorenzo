ALTER TABLE olist_customers_dataset RENAME TO olist_customers;
ALTER TABLE olist_order_payments_dataset RENAME TO olist_order_payments;
ALTER TABLE olist_geolocation_dataset RENAME TO olist_geolocation;
ALTER TABLE olist_geolocation_dataset_bis RENAME TO olist_geolocation_bis;
ALTER TABLE olist_order_items_dataset RENAME TO olist_order_items;
ALTER TABLE olist_order_reviews_dataset RENAME TO olist_order_reviews;
ALTER TABLE olist_orders_dataset RENAME TO olist_orders;
ALTER TABLE olist_products_dataset RENAME TO olist_products;
ALTER TABLE olist_sellers_dataset RENAME TO olist_sellers;

CREATE VIEW  customers_brazil as
(SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, region FROM olist_customers A LEFT JOIN brazil_states B ON (A.customer_state=B.abbreviation));

CREATE VIEW orders_customers as
SELECT order_id, A.customer_id, customer_unique_id, order_purchase_timestamp, customer_zip_code_prefix, customer_city, customer_state, region FROM olist_orders A LEFT JOIN customers_brazil B
using (customer_id);

create view olist_order_payments_groupby_order as
select order_id, sum(payment_value) as total_payment_value
from olist_order_payments
group by order_id;

CREATE VIEW orders_customers_payments as
SELECT A.order_id, customer_id, order_purchase_timestamp, customer_zip_code_prefix, customer_city, customer_state, region, total_payment_value FROM orders_customers A LEFT JOIN olist_order_payments_groupby_order B
using (order_id);

create view olist_order_items_groupby_order as
select order_id, count(product_id) as total_items_sold, sum(price) as total_price
from olist_order_items
group by order_id;

CREATE VIEW orders_customers_items as
SELECT A.order_id, customer_id, order_purchase_timestamp, customer_zip_code_prefix, customer_city, customer_state, region, total_items_sold, total_price FROM orders_customers A LEFT JOIN olist_order_items_groupby_order B
using (order_id);

CREATE VIEW new_customers_month as
SELECT date_trunc('month', order_purchase_timestamp) as purchase_month, customer_state, customer_unique_id, count(customer_unique_id) as nb_purchases
FROM orders_customers
group by purchase_month, customer_state, customer_unique_id
having count(customer_unique_id) = 1;

CREATE VIEW repurchases_month as
SELECT date_trunc('month', order_purchase_timestamp) as purchase_month, customer_state, customer_unique_id, count(customer_unique_id) as nb_purchases
FROM orders_customers
group by purchase_month, customer_state, customer_unique_id
having count(customer_unique_id) > 1;

CREATE VIEW customer_total_price as
SELECT customer_id, 0.03*sum(total_price) as margin FROM orders_customers_items
group by customer_id;
SELECT avg(margin)
FROM customer_total_price;

        avg        
-------------------
 4.132622291365842
