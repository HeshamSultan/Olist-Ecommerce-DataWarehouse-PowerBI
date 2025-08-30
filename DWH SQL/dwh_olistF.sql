DROP TABLE IF EXISTS dim_customers;
CREATE TABLE dim_customers (
    customer_sk INT PRIMARY KEY,
    customer_id VARCHAR UNIQUE,
    customer_unique_id VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR,
    customer_zip_code_prefix VARCHAR
);

DROP TABLE IF EXISTS dim_sellers;
CREATE TABLE dim_sellers (
    seller_sk INT PRIMARY KEY,
    seller_id VARCHAR UNIQUE,
    seller_city VARCHAR,
    seller_state VARCHAR,
    seller_zip_code_prefix VARCHAR
);

DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products (
    product_sk INT PRIMARY KEY,
    product_id VARCHAR UNIQUE,
    product_category_english_name VARCHAR,
    product_category_name VARCHAR,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT
);

DROP TABLE IF EXISTS dim_reviews;
CREATE TABLE dim_reviews (
    review_sk INT PRIMARY KEY,
    review_id VARCHAR UNIQUE,
    review_score INT,
    review_comment_title VARCHAR,
    review_comment_message VARCHAR,
    review_creation_date DATE,
    review_answer_timestamp TIMESTAMP
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_sk INT PRIMARY KEY,
    date DATE UNIQUE,
    day INT,
    day_of_week INT,
    month INT,
    year INT,
    quarter INT
);


DROP TABLE IF EXISTS fact_orderlifecycle;
CREATE TABLE fact_orderlifecycle (
    order_sk INT PRIMARY KEY,
    order_id VARCHAR,
    order_item_id INT,
    customer_sk INT REFERENCES dim_customers(customer_sk),
    product_sk INT REFERENCES dim_products(product_sk),
    seller_sk INT REFERENCES dim_sellers(seller_sk),
    order_purchase_date_sk INT REFERENCES dim_date(date_sk),
    order_approved_date_sk INT REFERENCES dim_date(date_sk),
    shipping_limit_date_sk INT REFERENCES dim_date(date_sk),
    delivered_carrier_date_sk INT REFERENCES dim_date(date_sk),
    delivered_customer_date_sk INT REFERENCES dim_date(date_sk),
    estimated_delivery_date_sk INT REFERENCES dim_date(date_sk),
    review_sk INT REFERENCES dim_reviews(review_sk),
    order_status VARCHAR,
    shipping_limit_date TIMESTAMP,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
	total_price FLOAT
);

DROP TABLE IF EXISTS fact_orderpayments;
CREATE TABLE fact_orderpayments (
    payment_sk INT PRIMARY KEY,
    order_id VARCHAR,
    payment_sequential INT,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value FLOAT,
    customer_sk INT REFERENCES dim_customers(customer_sk),
    payment_date_sk INT REFERENCES dim_date(date_sk),        
    payment_timestamp TIMESTAMP                              
);