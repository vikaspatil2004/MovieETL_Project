
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lower, regexp_replace

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load ratings, movies, and reviews CSVs from S3
ratings_df = spark.read.option("header", "true").csv("s3://your-bucket/input/ratings.csv")
movies_df = spark.read.option("header", "true").csv("s3://your-bucket/input/movies.csv")
reviews_df = spark.read.option("header", "true").csv("s3://your-bucket/input/reviews.csv")

# Drop rows with missing values
ratings_df = ratings_df.dropna()
reviews_df = reviews_df.dropna()

# Normalize review text: lowercase and remove special characters
reviews_df = reviews_df.withColumn("review_text",
    regexp_replace(lower(col("review_text")), "[^a-zA-Z0-9\\s]", ""))

# Join ratings with movies on movieId
ratings_movies_df = ratings_df.join(movies_df, on="movieId", how="inner")

# Join the result with reviews on movieId and userId
final_df = ratings_movies_df.join(reviews_df, on=["movieId", "userId"], how="inner")

# Write output to Parquet in S3
final_df.write.mode("overwrite").parquet("s3://your-bucket/output/cleaned_movie_data")

job.commit()
