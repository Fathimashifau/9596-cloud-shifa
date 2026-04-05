import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, from_json, to_timestamp, concat_ws, trim, length
)
from pyspark.sql.types import (
    StructType, StringType, ArrayType, BooleanType, IntegerType
)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

article_schema = StructType() \
    .add("article_id",      StringType()) \
    .add("link",            StringType()) \
    .add("title",           StringType()) \
    .add("description",     StringType()) \
    .add("content",         StringType()) \
    .add("keywords",        ArrayType(StringType())) \
    .add("creator",         ArrayType(StringType())) \
    .add("language",        StringType()) \
    .add("country",         ArrayType(StringType())) \
    .add("category",        ArrayType(StringType())) \
    .add("datatype",        StringType()) \
    .add("pubDate",         StringType()) \
    .add("fetched_at",      StringType()) \
    .add("image_url",       StringType()) \
    .add("video_url",       StringType()) \
    .add("source_id",       StringType()) \
    .add("source_name",     StringType()) \
    .add("source_priority", IntegerType()) \
    .add("source_url",      StringType()) \
    .add("duplicate",       BooleanType())

df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS")) \
    .option("subscribe", "news_topic") \
    .option("startingOffsets", "earliest") \
    .load()

print(f"Total raw records from Kafka: {df_raw.count()}")

df_parsed = df_raw \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), article_schema).alias("a")) \
    .select("a.*")

df_cleaned = df_parsed \
    .withColumn("pubDate",     to_timestamp(col("pubDate"),    "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("fetched_at",  to_timestamp(col("fetched_at"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("category",    concat_ws(",", col("category"))) \
    .withColumn("country",     concat_ws(",", col("country"))) \
    .withColumn("keywords",    concat_ws(",", col("keywords"))) \
    .withColumn("creator",     concat_ws(",", col("creator"))) \
    .withColumn("title",       trim(col("title"))) \
    .withColumn("description", trim(col("description"))) \
    .withColumn("link",        trim(col("link"))) \
    .filter(col("article_id").isNotNull()) \
    .filter(col("title").isNotNull()) \
    .filter(length(col("title")) > 0) \
    .filter(col("duplicate") == False) \
    .dropDuplicates(["article_id"])

df_final = df_cleaned.select(
    "article_id", "title", "description", "link",
    "pubDate", "fetched_at", "language", "category",
    "country", "keywords", "creator", "datatype",
    "source_id", "source_name", "source_priority",
    "source_url", "image_url", "video_url"
)

print(f"Total cleaned records: {df_final.count()}")

df_final.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{os.environ.get('RDS_HOST')}:5432/postgres") \
    .option("dbtable", "news_articles") \
    .option("user", os.environ.get("RDS_USER")) \
    .option("password", os.environ.get("RDS_PASSWORD")) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Data written to RDS successfully!")
job.commit()