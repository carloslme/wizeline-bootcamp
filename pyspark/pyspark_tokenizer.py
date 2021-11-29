from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import BooleanType, IntegerType

df = spark.read.options(header=True).csv("gs://raw-layer-330021/movie_review.csv")

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
reviews.write.parquet("gs://staging-layer-330021/reviews.parquet")
