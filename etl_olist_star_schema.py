"""
ETL Job: etl-olist-star-schema
AWS Glue ETL script that reads raw Olist CSV files from S3 and writes
a star schema in Parquet format to the processed/ prefix.

Source:      s3://olist-ecommerce-ca-dc/raw/
Destination: s3://olist-ecommerce-ca-dc/processed/
Database:    olist_catalog (AWS Glue Data Catalog)
Region:      ca-central-1
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "s3://olist-ecommerce-ca-dc"
RAW = f"{BUCKET}/raw"
PROCESSED = f"{BUCKET}/processed"

# ---------------------------------------------------------------------------
# Read raw CSVs
# ---------------------------------------------------------------------------

orders = spark.read.option("header", "true").csv(f"{RAW}/olist_orders_dataset.csv")
order_items = spark.read.option("header", "true").csv(f"{RAW}/olist_order_items_dataset.csv")
customers = spark.read.option("header", "true").csv(f"{RAW}/olist_customers_dataset.csv")
products = spark.read.option("header", "true").csv(f"{RAW}/olist_products_dataset.csv")
sellers = spark.read.option("header", "true").csv(f"{RAW}/olist_sellers_dataset.csv")
payments = spark.read.option("header", "true").csv(f"{RAW}/olist_order_payments_dataset.csv")
reviews = spark.read.option("header", "true").csv(f"{RAW}/olist_order_reviews_dataset.csv")
geo = spark.read.option("header", "true").csv(f"{RAW}/olist_geolocation_dataset.csv")
category_translation = spark.read.option("header", "true").csv(f"{RAW}/product_category_name_translation.csv")

# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------

dates = (
    orders
    .select(F.to_date("order_purchase_timestamp").alias("date"))
    .distinct()
    .filter(F.col("date").isNotNull())
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("week", F.weekofyear("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]).cast("boolean"))
)

dates.write.mode("overwrite").parquet(f"{PROCESSED}/dim_date/")

# ---------------------------------------------------------------------------
# dim_customer
# ---------------------------------------------------------------------------

geo_customer = (
    geo
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.avg("geolocation_lat").alias("geo_lat"),
        F.avg("geolocation_lng").alias("geo_lng")
    )
)

dim_customer = (
    customers
    .join(geo_customer,
          customers.customer_zip_code_prefix == geo_customer.geolocation_zip_code_prefix,
          "left")
    .select(
        F.monotonically_increasing_id().alias("customer_key"),
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix",
        "geo_lat",
        "geo_lng"
    )
)

dim_customer.write.mode("overwrite").parquet(f"{PROCESSED}/dim_customer/")

# ---------------------------------------------------------------------------
# dim_product
# ---------------------------------------------------------------------------

dim_product = (
    products
    .join(category_translation,
          products.product_category_name == category_translation.product_category_name,
          "left")
    .select(
        F.monotonically_increasing_id().alias("product_key"),
        "product_id",
        products.product_category_name.alias("category_name_pt"),
        F.col("product_category_name_english").alias("category_name_en"),
        "product_weight_g",
        "product_photos_qty",
        "product_name_lenght"
    )
)

dim_product.write.mode("overwrite").parquet(f"{PROCESSED}/dim_product/")

# ---------------------------------------------------------------------------
# dim_seller
# ---------------------------------------------------------------------------

geo_seller = (
    geo
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.avg("geolocation_lat").alias("geo_lat"),
        F.avg("geolocation_lng").alias("geo_lng")
    )
)

dim_seller = (
    sellers
    .join(geo_seller,
          sellers.seller_zip_code_prefix == geo_seller.geolocation_zip_code_prefix,
          "left")
    .select(
        F.monotonically_increasing_id().alias("seller_key"),
        "seller_id",
        "seller_city",
        "seller_state",
        "seller_zip_code_prefix",
        "geo_lat",
        "geo_lng"
    )
)

dim_seller.write.mode("overwrite").parquet(f"{PROCESSED}/dim_seller/")

# ---------------------------------------------------------------------------
# dim_payment
# ---------------------------------------------------------------------------

dim_payment = (
    payments
    .groupBy("order_id")
    .agg(
        F.first("payment_type").alias("payment_type"),
        F.sum("payment_installments").cast(IntegerType()).alias("payment_installments"),
        F.sum("payment_value").cast(DoubleType()).alias("payment_value"),
        F.max("payment_sequential").cast(IntegerType()).alias("payment_sequential")
    )
    .withColumn("payment_key", F.monotonically_increasing_id())
)

dim_payment.write.mode("overwrite").parquet(f"{PROCESSED}/dim_payment/")

# ---------------------------------------------------------------------------
# dim_geography (disconnected — duplicate zip codes prevent clean join)
# ---------------------------------------------------------------------------

dim_geography = (
    geo
    .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
    .withColumnRenamed("geolocation_city", "city")
    .withColumnRenamed("geolocation_state", "state")
    .withColumnRenamed("geolocation_lat", "geo_lat")
    .withColumnRenamed("geolocation_lng", "geo_lng")
    .withColumn("geo_key", F.monotonically_increasing_id())
    .select("geo_key", "zip_code_prefix", "city", "state", "geo_lat", "geo_lng")
)

dim_geography.write.mode("overwrite").parquet(f"{PROCESSED}/dim_geography/")

# ---------------------------------------------------------------------------
# fact_orders
# ---------------------------------------------------------------------------

# Aggregate reviews to one row per order (avg score)
reviews_agg = (
    reviews
    .groupBy("order_id")
    .agg(F.avg("review_score").cast(DoubleType()).alias("review_score"))
)

fact_orders = (
    order_items
    .join(orders, "order_id", "inner")
    .join(dim_customer.select("customer_key", "customer_id"),
          orders.customer_id == dim_customer.customer_id, "left")
    .join(dim_product.select("product_key", "product_id"),
          order_items.product_id == dim_product.product_id, "left")
    .join(dim_seller.select("seller_key", "seller_id"),
          order_items.seller_id == dim_seller.seller_id, "left")
    .join(dim_payment.select("payment_key", "order_id"),
          orders.order_id == dim_payment.order_id, "left")
    .join(dates.select("date_key", "date"),
          F.to_date(orders.order_purchase_timestamp) == dates.date, "left")
    .join(reviews_agg, "order_id", "left")
    .withColumn("price", F.col("price").cast(DoubleType()))
    .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
    .withColumn("total_value", F.col("price") + F.col("freight_value"))
    .withColumn("days_to_deliver",
        F.datediff(
            F.to_date("order_delivered_customer_date"),
            F.to_date("order_purchase_timestamp")
        ).cast(IntegerType())
    )
    .withColumn("delay_days",
        F.datediff(
            F.to_date("order_delivered_customer_date"),
            F.to_date("order_estimated_delivery_date")
        ).cast(IntegerType())
    )
    .select(
        "order_id",
        F.col("order_item_id").cast(IntegerType()),
        "customer_key",
        "product_key",
        "seller_key",
        "payment_key",
        "date_key",
        "order_status",
        "price",
        "freight_value",
        "total_value",
        "days_to_deliver",
        "delay_days",
        "review_score",
        F.col("order_purchase_timestamp").alias("order_purchase_timestamp"),
        F.col("order_delivered_customer_date").alias("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date").alias("order_estimated_delivery_date")
    )
)

fact_orders.write.mode("overwrite").parquet(f"{PROCESSED}/fact_orders/")

job.commit()
