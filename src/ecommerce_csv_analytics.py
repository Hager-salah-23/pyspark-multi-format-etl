# ============================================================
# Ecommerce Analytics - Full Join Script (PySpark + spark.sql)
# Final Working Version
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys
from datetime import datetime

# ============================================================
# LOGGING - Save ALL terminal output to a text file
# ============================================================
log_filename = f"ecommerce_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger(log_filename)
print(f"All terminal output is being saved to: {log_filename}")
print("=" * 70)


# ------------------------------------------------------------
# 1. Create Spark Session
# ------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Ecommerce_Sales_Analysis")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------
# 2. Load all CSV files from HDFS
# ------------------------------------------------------------
print("Loading data from HDFS...")

BASE_PATH = "hdfs://localhost:9000/user/hadoop/project"

order_items = spark.read.csv(f"{BASE_PATH}/order_items.csv", header=True, inferSchema=True)
sales       = spark.read.csv(f"{BASE_PATH}/ecommerce_sales_customer_analytics_150k.csv", header=True, inferSchema=True)
products    = spark.read.csv(f"{BASE_PATH}/product_catalog.csv", header=True, inferSchema=True)
customers   = spark.read.csv(f"{BASE_PATH}/customer_master.csv", header=True, inferSchema=True)

print("Data loaded successfully!")
print(f"order_items   : {order_items.count():,} rows")
print(f"sales         : {sales.count():,} rows")
print(f"products      : {products.count():,} rows")
print(f"customers     : {customers.count():,} rows")

# ------------------------------------------------------------
# 3. Register Temporary Views
# ------------------------------------------------------------
order_items.createOrReplaceTempView("order_items")
sales.createOrReplaceTempView("sales")
products.createOrReplaceTempView("products")
customers.createOrReplaceTempView("customers")

# ------------------------------------------------------------
# 4. Create Full Enriched Table
# ------------------------------------------------------------
print("\nCreating enriched table...")

enriched = spark.sql("""
    SELECT 
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.discount_percentage,
        oi.discount_amount,
        oi.gross_sales,
        oi.tax_amount,
        oi.shipping_cost,
        oi.net_sales,
        oi.product_cost,
        oi.profit AS item_profit,

        p.product_name,
        p.product_category,
        p.product_subcategory,
        p.brand,
        p.supplier,
        p.product_rating,

        s.order_date,
        s.order_time,
        s.order_status,
        s.sales_channel,
        s.payment_method,
        s.payment_status,
        s.currency,
        s.shipping_method,
        s.warehouse,
        s.delivery_days,
        s.delivery_status,
        s.return_status,
        s.return_reason,
        s.customer_rating,
        s.review_sentiment,
        s.marketing_channel,
        s.campaign_name,
        s.coupon_code,
        s.loyalty_points_earned,
        s.loyalty_points_redeemed,
        s.profit_margin_percentage,
        s.customer_lifetime_value,
        s.is_repeat_customer,
        s.customer_order_count,

        s.customer_id,
        s.customer_name,
        s.customer_age,
        s.gender,
        s.customer_segment,
        s.customer_type,
        s.customer_city,
        s.customer_state,
        s.customer_country,
        s.region,
        c.customer_acquisition_cost

    FROM order_items oi
    LEFT JOIN products p ON oi.product_id = p.product_id
    LEFT JOIN sales s ON oi.order_id = s.order_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
""")

enriched.createOrReplaceTempView("enriched_sales")
print(f"Enriched table created with {enriched.count():,} rows")

# ------------------------------------------------------------
# 5. Analysis Queries
# ------------------------------------------------------------

print("\n=== Top 10 Products by Revenue ===")
spark.sql("""
    SELECT 
        product_name,
        product_category,
        brand,
        SUM(quantity) AS total_units,
        ROUND(SUM(net_sales), 2) AS total_revenue,
        ROUND(SUM(item_profit), 2) AS total_profit
    FROM enriched_sales
    GROUP BY product_name, product_category, brand
    ORDER BY total_revenue DESC
    LIMIT 10
""").show(truncate=False)

print("\n=== Revenue by Category ===")
spark.sql("""
    SELECT 
        product_category,
        COUNT(DISTINCT order_id) AS orders,
        SUM(quantity) AS units_sold,
        ROUND(SUM(net_sales), 2) AS revenue,
        ROUND(SUM(item_profit), 2) AS profit,
        ROUND(AVG(product_rating), 2) AS avg_rating
    FROM enriched_sales
    GROUP BY product_category
    ORDER BY revenue DESC
""").show(truncate=False)

print("\n=== Customer Segment Performance ===")
spark.sql("""
    SELECT 
        customer_segment,
        COUNT(DISTINCT customer_id) AS customers,
        COUNT(DISTINCT order_id) AS orders,
        ROUND(SUM(net_sales), 2) AS revenue,
        ROUND(AVG(customer_lifetime_value), 2) AS avg_clv
    FROM enriched_sales
    GROUP BY customer_segment
    ORDER BY revenue DESC
""").show(truncate=False)

print("\n=== Monthly Revenue Trend ===")
spark.sql("""
    SELECT 
        date_format(order_date, 'yyyy-MM') AS month,
        COUNT(DISTINCT order_id) AS orders,
        ROUND(SUM(net_sales), 2) AS revenue,
        ROUND(SUM(item_profit), 2) AS profit
    FROM enriched_sales
    GROUP BY date_format(order_date, 'yyyy-MM')
    ORDER BY month
""").show(60, truncate=False)

# ------------------------------------------------------------
# 6. Save Enriched Table
# ------------------------------------------------------------
print("\nSaving enriched table as Parquet...")
enriched.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/project/output/enriched_sales")

print("\n=== Script completed successfully! ===")
print(f"\n>>> Full terminal log saved to: {log_filename}")
