from pyspark.sql import SparkSession

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

# Implement logicxs
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
    GROUP BY 1;
    """
)

# Saving the data to BigQuery
logic.write.format('bigquery') \
  .mode("append") \
  .option("temporaryGcsBucket","wizeline-bootcamp-330020") \
  .option('table', 'dwh.user_behavior_metric') \
  .save()