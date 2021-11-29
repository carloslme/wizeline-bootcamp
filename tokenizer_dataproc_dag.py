from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# Default arguments
default_args = {
    "owner": "carlos.lopez",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}

CLUSTER_NAME = "dataproc-cluster"
REGION = "us-central1"
PROJECT_ID = "wizeline-bootcamp-330020"
PYSPARK_URI = "gs://wizeline-bootcamp-330020/pyspark_tokenyzer.py"

# Cluster definition
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with DAG(
    dag_id="classification_movie_review_dataproc",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    create_cluster >> pyspark_task >> delete_cluster


from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import BooleanType, IntegerType

df = spark.read.options(header=True).csv("/movie_review.csv")

# Tokenizing the words
regexTokenizer = RegexTokenizer(
    inputCol="review_str", outputCol="review_token", pattern="\\W"
)
regexTokenized = regexTokenizer.transform(df)

# Removing stop words
remover = StopWordsRemover(
    inputCol="review_token",
    outputCol="filtered",
    # stopWords=stopwordList
)

clean_df = remover.transform(regexTokenized).select("cid", "filtered")

# Filter positive reviews
word_good = udf(lambda words: "good" in words, BooleanType())
reviews_bool = clean_df.withColumn(
    "positive_review_bool", word_good(col("filtered"))
).select("cid", "positive_review_bool")

# Converting "positive_review_bool" column from boolean to int
reviews = reviews_bool.withColumn(
    "positive_review", when(reviews_bool.positive_review_bool == True, 1).otherwise(0)
).select(reviews_bool.cid.alias("user_id"), "positive_review")

# Saving data frame as parquet
reviews.write.parquet("gs://BUCKET-NAME-HERE/reviews.parquet")
