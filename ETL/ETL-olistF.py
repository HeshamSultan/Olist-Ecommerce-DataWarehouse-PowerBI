import findspark 
findspark.init()
import pyspark.sql 
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, avg, first, col, monotonically_increasing_id, year, month,dayofmonth, dayofweek, quarter, expr
from pyspark.sql.types import DateType


# ----------------------------------------------
# ETL Pipeline for Olist Brazil E-Commerce Dataset
# Step 1: Extract & Clean
# Step 2: Ensure Referential Integrity
# Step 3: Transform into Dim/Fact tables
# Step 4: Load into PostgreSQL Data Warehouse
# ----------------------------------------------

def create_spark_session():
    return SparkSession.builder \
        .appName("Full_ETL_Pipeline") \
        .config("spark.jars", "/home/hesham/postgresql-42.7.3.jar") \
        .getOrCreate()


def read_raw_data(spark, path_root):
    s = """
        review_id string,
        order_id string,
        review_score int,
        review_comment_title string,
        review_comment_message string,
        review_creation_date date,
        review_answer_timestamp timestamp
        """
    raw = {}
    raw['customers'] = spark.read.option("header", True).csv(f"{path_root}/olist_customers_dataset.csv")
    raw['orders'] = spark.read.option("header", True).csv(f"{path_root}/olist_orders_dataset.csv")
    raw['order_items'] = spark.read.option("header", True).csv(f"{path_root}/olist_order_items_dataset.csv")
    raw['products'] = spark.read.option("header", True).csv(f"{path_root}/olist_products_dataset.csv")
    raw['sellers'] = spark.read.option("header", True).csv(f"{path_root}/olist_sellers_dataset.csv")
    raw['geolocation'] = spark.read.option("header", True).csv(f"{path_root}/olist_geolocation_dataset.csv")
    raw['payments'] = spark.read.option("header", True).csv(f"{path_root}/olist_order_payments_dataset.csv")
    raw['reviews'] = spark.read.option("header", True).schema(s).csv(f"{path_root}/olist_order_reviews_dataset.csv")
    raw['categories'] = spark.read.option("header", True).csv(f"{path_root}/product_category_name_translation.csv")
    return raw


def clean_data(raw):
    clean = {}
    clean['customers'] = raw['customers'].dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])
    clean['orders'] = raw['orders'].dropna(subset=["order_id","customer_id"])\
        .withColumn("order_purchase_timestamp", col("order_purchase_timestamp").cast("timestamp"))
    clean['order_items'] = raw['order_items'].dropna(subset=["order_id","order_item_id"])\
        .withColumn("price", col("price").cast("double"))\
        .withColumn("freight_value", col("freight_value").cast("double"))
    clean['categories'] = raw['categories'].dropDuplicates(["product_category_name"])
    clean['products'] = raw['products'].dropDuplicates(["product_id"]).join(clean['categories'], on= 'product_category_name', how= 'inner')
    clean['sellers'] = raw['sellers'].dropDuplicates(["seller_id"])
    clean['geolocation'] = raw['geolocation']\
        .groupBy("geolocation_zip_code_prefix")\
        .agg(
            first("geolocation_city").alias("geolocation_city"),
            first("geolocation_state").alias("geolocation_state"),
            avg("geolocation_lat").alias("avg_latitude"),
            avg("geolocation_lng").alias("avg_longitude")
        )
    clean['payments'] = raw['payments'].dropDuplicates(['order_id','payment_sequential']).dropna(subset=["order_id"])
    clean['reviews'] = raw['reviews'].dropDuplicates(['review_id']).dropna(subset=["order_id"])
    # .withColumn("review_creation_date", col("review_creation_date").cast("date"))\
    # .withColumn("review_answer_timestamp", col("review_answer_timestamp").cast("timestamp"))\
    # .withColumn("review_score" , col("review_score").cast("integer"))
   
    return clean


def check_referential_integrity(clean):
    # Identify orphan seller_ids in order_items not present in sellers
    orphan_sellers = clean['order_items'].select('seller_id').distinct() \
        .join(clean['sellers'].select('seller_id'), on='seller_id', how='left_anti')
    if not orphan_sellers.rdd.isEmpty():
        print("Orphan seller_ids in order_items:")
        orphan_sellers.show()
    # Identify orphan product_ids in order_items not present in products
    orphan_products = clean['order_items'].select('product_id').distinct() \
        .join(clean['products'].select('product_id'), on='product_id', how='left_anti')
    if not orphan_products.rdd.isEmpty():
        print("Orphan product_ids in order_items:")
        orphan_products.show()
    # Identify orphan customer_zip_code_prefix in customers not in geolocation
    orphan_customers = clean['customers'].select('customer_zip_code_prefix').distinct() \
        .join(clean['geolocation'].select('geolocation_zip_code_prefix'), 
              clean['customers']['customer_zip_code_prefix'] == clean['geolocation']['geolocation_zip_code_prefix'], 
              how='left_anti')
    if not orphan_customers.rdd.isEmpty():
        print("Orphan customer_zip_code_prefix in customers:")
        orphan_customers.show()
    # Identify orphan order_ids in orders not present in order_items
    orphan_orders = clean['orders'].select('order_id').distinct() \
        .join(clean['order_items'].select('order_id').distinct(), on='order_id', how='left_anti')
    if not orphan_orders.rdd.isEmpty():
        print("Orphan order_ids in orders (no items):")
        orphan_orders.show()


def ensure_referential_integrity(clean):
    # Drop order_items with invalid product_id
    clean['order_items'] = clean['order_items'] \
        .join(clean['products'].select('product_id'), on='product_id', how='inner')
    # Keep only valid geolocation for sellers
    clean['sellers'] = clean['sellers'].join(
        clean['geolocation'], 
        clean['sellers']['seller_zip_code_prefix'] == clean['geolocation']['geolocation_zip_code_prefix'], 
        how='inner')
    # drop invalid seller_id in order_items_clean
    clean['order_items'] = clean['order_items'].join(clean['sellers'] , on = "seller_id" , how = "inner" ) 
    # Keep only valid geolocation for customers
    clean['customers'] = clean['customers'].join(
        clean['geolocation'],
        clean['customers']['customer_zip_code_prefix'] == clean['geolocation']['geolocation_zip_code_prefix'],
        how='inner')
    # Drop orders related to invalid customers
    clean['orders'] = clean['orders'].join(
        clean['customers'].select('customer_id'), on='customer_id', how='inner')
    # Drop order_items, reviews, payments for invalid orders
    valid_orders = clean['orders'].select('order_id')
    clean['order_items'] = clean['order_items'].join(valid_orders, on='order_id', how='inner')
    clean['reviews'] = clean['reviews'].join(valid_orders, on='order_id', how='inner')
    clean['payments'] = clean['payments'].join(valid_orders, on='order_id', how='inner')
    return clean


def transform_data(clean,spark):
  
    dims = {}
    facts = {}

    # ----------------------
    # Dimension: Customers                    
    # ----------------------
    w_c = Window().orderBy("customer_id")  
    dims['dim_customers'] = clean['customers']\
        .withColumn("customer_sk", F.row_number().over(w_c)) \
        .select(
            "customer_sk","customer_id", "customer_unique_id",
            "customer_city", "customer_state", "customer_zip_code_prefix"
        )

    # ----------------------
    # Dimension: Sellers                    
    # ----------------------
    w_s = Window().orderBy("seller_id")  
    dims['dim_sellers'] = clean['sellers']\
        .withColumn("seller_sk", F.row_number().over(w_s)) \
        .select(
             "seller_sk","seller_id", "seller_city",
            "seller_state", "seller_zip_code_prefix"
        )

    # ----------------------
    # Dimension: Products                 
    # ----------------------
    w_p = Window().orderBy("product_id")  
    dims['dim_products'] = clean['products']\
        .withColumn("product_sk", F.row_number().over(w_p))\
        .withColumn("product_name_lenght" , col("product_name_lenght").cast('integer'))\
        .withColumn("product_description_lenght" , col("product_description_lenght").cast('integer'))\
        .withColumn("product_photos_qty" , col("product_photos_qty").cast('integer'))\
        .withColumn("product_weight_g" , col("product_weight_g").cast('double'))\
        .withColumn("product_length_cm" , col("product_length_cm").cast('double'))\
        .withColumn("product_height_cm" , col("product_height_cm").cast('double'))\
        .withColumn("product_width_cm" , col("product_width_cm").cast('double'))\
        .select(
            "product_sk","product_id",
            col("product_category_name_english").alias("product_category_english_name"),
            "product_category_name", col("product_name_lenght").alias("product_name_length"),
            col("product_description_lenght").alias("product_description_length"), "product_photos_qty",
            "product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm"
        )

    # ----------------------
    # Dimension: Reviews                  
    # ----------------------
    w_r = Window().orderBy("review_id")  
    dims['dim_reviews'] = clean['reviews'] \
        .withColumn("review_sk", F.row_number().over(w_r)) \
        .select(
            "review_sk","review_id", "review_score",
            "review_comment_title", "review_comment_message",
            "review_creation_date", "review_answer_timestamp"
        )

    # ----------------------
    # Dimension: Date
    # ----------------------
    min_date = clean['orders'].agg({"order_purchase_timestamp": "min"}).first()[0].date()
    max_date = clean['orders'].agg({"order_purchase_timestamp": "max"}).first()[0].date()

    date_range = (
    spark.range(1, (max_date - min_date).days + 1)
    .withColumn("date", expr(f"date_add(date('{min_date}'), CAST(id AS INT))")))
    w_d = Window().orderBy("date") 
    dims['dim_date'] = date_range \
        .withColumn("date_sk", F.row_number().over(w_d) ) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("day_of_week", dayofweek("date")) \
        .withColumn("month", month("date")) \
        .withColumn("year", year("date")) \
        .withColumn("quarter", quarter("date")) \
        .select("date_sk", "date", "day", "day_of_week", "month", "year", "quarter")

    # ----------------------
    # Fact: Order Lifecycle
    # ----------------------
    fact_df = clean['order_items'] \
    .join(clean['orders'], "order_id") \
    .join(dims['dim_customers'].select("customer_id", "customer_sk"), "customer_id") \
    .join(dims['dim_products'].select("product_id", "product_sk"), "product_id") \
    .join(dims['dim_sellers'].select("seller_id", "seller_sk"), "seller_id") \
    .join(clean['reviews'].select("order_id", "review_id"), "order_id", "left") \
    .join(dims['dim_reviews'].select("review_id", "review_sk"), "review_id", "left")

    # Extract dates from the timestamp columns
    fact_df = (
        fact_df
        .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
        .withColumn("order_approved_date", F.to_date("order_approved_at"))
        .withColumn("delivered_carrier_date", F.to_date("order_delivered_carrier_date"))
        .withColumn("delivered_customer_date", F.to_date("order_delivered_customer_date"))
        .withColumn("estimated_delivery_date", F.to_date("order_estimated_delivery_date"))
        .withColumn("shipping_limit_date", F.to_date("shipping_limit_date"))
    )
    
    # For each event, join to the date dimension, adding a _sk column
    for date_col, sk_col in [
        ("order_purchase_date", "order_purchase_date_sk"),
        ("order_approved_date", "order_approved_date_sk"),
        ("delivered_carrier_date", "delivered_carrier_date_sk"),
        ("delivered_customer_date", "delivered_customer_date_sk"),
        ("estimated_delivery_date", "estimated_delivery_date_sk"),
        ("shipping_limit_date", "shipping_limit_date_sk")
    ]:
        fact_df = fact_df.join(
            dims['dim_date'].select(F.col("date").alias(date_col), F.col("date_sk").alias(sk_col)),
            on=[date_col],
            how="left"
        )
    
    # Final select, include all surrogate/date keys and measures
    w_fact = Window().orderBy("order_id", "order_item_id")  
    facts['fact_orderlifecycle'] = fact_df.withColumn("order_sk",F.row_number().over(w_fact))\
        .withColumn("order_item_id" , col("order_item_id").cast('integer'))\
        .withColumn("order_approved_at" , col("order_approved_at").cast('timestamp'))\
        .withColumn("order_delivered_carrier_date" , col("order_delivered_carrier_date").cast('timestamp'))\
        .withColumn("order_delivered_customer_date" , col("order_delivered_customer_date").cast('timestamp'))\
        .withColumn("order_estimated_delivery_date" , col("order_estimated_delivery_date").cast('timestamp'))\
        .select(
        "order_sk",   
        "order_id",
        "order_item_id",
        "customer_sk",
        "product_sk",
        "seller_sk",
        "order_purchase_date_sk",
        "order_approved_date_sk",
        "shipping_limit_date_sk",
        "delivered_carrier_date_sk",
        "delivered_customer_date_sk",
        "estimated_delivery_date_sk",
        "review_sk",
        "order_status",
        "shipping_limit_date",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "price",
        "freight_value",
        (col("price")+col("freight_value")).alias("total_price")
    )

    # ----------------------
    # Fact: Order Payments
    # ----------------------
    fact_payments = clean['payments'] \
    .join(clean['orders'].select("order_id", "customer_id", "order_purchase_timestamp"),
          "order_id") \
    .join(dims['dim_customers'].select("customer_id", "customer_sk"), "customer_id") \
    .withColumn("payment_date", F.to_date("order_purchase_timestamp")) \
    .withColumn("payment_sequential", col("payment_sequential").cast('integer'))\
    .withColumn("payment_installments", col("payment_installments").cast('integer'))\
    .withColumn("payment_value", col("payment_value").cast('double'))\
    .join(dims['dim_date'].select(F.col("date").alias("payment_date"), F.col("date_sk").alias("payment_date_sk")),
          on="payment_date", how="left")

    # Now build the fact table with all relevant keys and metrics
    w_fact_p = Window().orderBy("order_id", "payment_sequential")  
    facts['fact_orderpayments'] = fact_payments.withColumn("payment_sk", F.row_number().over(w_fact_p))\
        .select(
        "payment_sk",  
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "customer_sk",
        "payment_date_sk",
        col("order_purchase_timestamp").alias("payment_timestamp"))

    return dims, facts



def load_to_postgres(dims, facts, jdbc_url, props):
    for name, df in dims.items():
        df.write.jdbc(url=jdbc_url, table=name, mode="append", properties=props)
    for name, df in facts.items():
        df.write.jdbc(url=jdbc_url, table=name, mode="append", properties=props)


def main():
    spark = create_spark_session()
    raw = read_raw_data(spark, "/home/hesham/ITI/Grad Project/archive_restored")
    clean = clean_data(raw)
    clean = ensure_referential_integrity(clean)
    dims, facts = transform_data(clean,spark)
    jdbc_url = "jdbc:postgresql://192.168.150.1:5432/dwh_olistF"
    props = {"user": "postgres", "password": "mypass123", "driver": "org.postgresql.Driver","truncate": "true"}
    load_to_postgres(dims, facts, jdbc_url, props)
    spark.stop()

if __name__ == "__main__":
    main()
