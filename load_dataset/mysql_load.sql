LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_items_dataset.csv' 
INTO TABLE testing.olist_order_items_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_payments_dataset.csv' 
INTO TABLE testing.olist_order_payments_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_orders_dataset.csv' 
INTO TABLE testing.olist_orders_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_products_dataset.csv' 
INTO TABLE testing.olist_products_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/tmp/dataset/olist_customers_dataset.csv' 
INTO TABLE testing.olist_customers_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_geolocation_dataset.csv' 
INTO TABLE testing.olist_geolocation_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_reviews_dataset.csv' 
-- INTO TABLE testing.olist_order_reviews_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_sellers_dataset.csv' 
INTO TABLE testing.olist_sellers_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;