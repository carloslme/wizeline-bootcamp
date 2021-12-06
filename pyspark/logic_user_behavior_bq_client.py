from pyspark.sql import SparkSession
from google.cloud.storage import client
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import io
import pandas as pd

spark = SparkSession.builder.getOrCreate()

BUCKET = "staging-layer-330021"

# Connection to GCS and get files
df_reviews = spark.read.options(header=True).parquet(
    "gs://staging-layer-330021/*.parquet"
)
df_user_purchase = spark.read.options(header=True).csv(
    "gs://staging-layer-330021/user_purchase.csv"
)

# Create views to query
df_reviews.createOrReplaceTempView("reviews")
df_user_purchase.createOrReplaceTempView("user_purchase")

# Implement logic
logic = spark.sql(
    """
    SELECT 
      CAST(up.customer_id AS STRING) AS customer
      , CAST(SUM(up.quantity * up.unit_price) AS STRING) as amount_spent
      , CAST(SUM(r.positive_review) AS STRING) as review_score
      , CAST(COUNT(r.user_id) AS STRING) as review_count
      , CAST(current_date() AS STRING) as insert_date
    FROM reviews AS r
    LEFT JOIN user_purchase AS up ON r.user_id == up.customer_id
    GROUP BY 1
    """
)

# Convert to Pandas dataframe
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
pd_df = logic.toPandas()


# Construct a BigQuery client object to send dataframe
client = bigquery.Client()
table_id = "wizeline-bootcamp-330020.dwh.user_behavior_metric"

job_config = bigquery.LoadJobConfig(
    schema=[
        SchemaField("customer", "STRING", "REQUIRED", None, ()),
        SchemaField("amount_spent", "STRING", "REQUIRED", None, ()),
        SchemaField("review_score", "STRING", "REQUIRED", None, ()),
        SchemaField("review_count", "STRING", "REQUIRED", None, ()),
        SchemaField("insert_date", "STRING", "REQUIRED", None, ()),
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

# Write dataframe
job = client.load_table_from_dataframe(
    pd_df, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
