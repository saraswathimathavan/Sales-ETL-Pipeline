
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ------------------------------------------------
# 1. INITIALIZE GLUE
# ------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------
# 2. S3 PATHS
# REPLACE WITH YOUR ACTUAL BUCKET NAME
# ------------------------------------------------

BUCKET = "sales-etl-ishu-2026"

INPUT_PATH = f"s3://{BUCKET}/raw/sales_data.csv"

PRODUCT_OUTPUT = f"s3://{BUCKET}/processed/dim_product/"

SALES_OUTPUT = f"s3://{BUCKET}/processed/fact_sales/"

REPORT_OUTPUT = f"s3://{BUCKET}/processed/sales_report/"

# ------------------------------------------------
# 3. EXTRACT DATA FROM S3
# ------------------------------------------------

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

print("Input record count:", df.count())
print("Input columns:", df.columns)

# ------------------------------------------------
# 4. VALIDATE REQUIRED COLUMNS
# ------------------------------------------------

required_columns = [
    "sale_id",
    "product_id",
    "product_name",
    "quantity",
    "unit_price"
]

missing = [
    col for col in required_columns
    if col not in df.columns
]

if missing:
    raise ValueError(
        f"Missing required CSV columns: {missing}"
    )

# ------------------------------------------------
# 5. TRANSFORM DATA
# ------------------------------------------------

df = (
    df
    .withColumn("sale_id", F.col("sale_id").cast("string"))
    .withColumn("product_id", F.col("product_id").cast("string"))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("quantity", F.col("quantity").cast("double"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
)

# Calculate sales amount
df = df.withColumn(
    "sales_amount",
    F.col("quantity") * F.col("unit_price")
)

# Filter out zero-dollar and invalid sales
sales = df.filter(
    (F.col("sales_amount") > 0) &
    F.col("sale_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("quantity").isNotNull() &
    F.col("unit_price").isNotNull()
)

print("Filtered sales count:", sales.count())

# ------------------------------------------------
# 6. CREATE PRODUCT DIMENSION TABLE
# ------------------------------------------------

dim_product = (
    sales
    .select(
        "product_id",
        "product_name"
    )
    .dropDuplicates(["product_id"])
)

# ------------------------------------------------
# 7. CREATE SALES FACT TABLE
# ------------------------------------------------

fact_sales = sales.select(
    "sale_id",
    "product_id",
    "quantity",
    "unit_price",
    "sales_amount"
)

# ------------------------------------------------
# 8. CREATE AGGREGATED SALES REPORT
# ------------------------------------------------

sales_report = (
    fact_sales
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("sales_amount").alias("total_sales"),
        F.count("sale_id").alias("number_of_sales")
    )
    .orderBy("product_id")
)

# ------------------------------------------------
# 9. LOAD DATA INTO S3
# ------------------------------------------------

dim_product.write \
    .mode("overwrite") \
    .parquet(PRODUCT_OUTPUT)

fact_sales.write \
    .mode("overwrite") \
    .parquet(SALES_OUTPUT)

sales_report.write \
    .mode("overwrite") \
    .parquet(REPORT_OUTPUT)

# ------------------------------------------------
# 10. DISPLAY FINAL RESULTS
# ------------------------------------------------

print("PRODUCT DIMENSION TABLE")
dim_product.show()

print("SALES FACT TABLE")
fact_sales.show()

print("AGGREGATED SALES REPORT")
sales_report.show()

print("ETL PIPELINE COMPLETED SUCCESSFULLY")

job.commit()
