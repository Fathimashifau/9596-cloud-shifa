
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, concat_ws, trim, length
)
from pyspark.sql.types import (
    StructType, StringType, ArrayType, BooleanType, IntegerType
)


spark = SparkSession.builder \
    .appName("KafkaNewsStreaming") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .config(
        "spark.driver.extraClassPath",
        "/home/shifa/kafka/kafka_2.13-3.8.1/postgresql-42.7.3.jar"
    ) \
    .config(
        "spark.executor.extraClassPath",
        "/home/shifa/kafka/kafka_2.13-3.8.1/postgresql-42.7.3.jar"
    ) \
    .getOrCreate()

# ─────────────────────────────────────────────
# 2. Schema
# ─────────────────────────────────────────────
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

# ─────────────────────────────────────────────
# 3. Read from Kafka
# ─────────────────────────────────────────────
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news-data") \
    .option("startingOffsets", "earliest") \
    .load()

# ─────────────────────────────────────────────
# 4. Parse JSON
# ─────────────────────────────────────────────
df_parsed = df_raw \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), article_schema).alias("a")) \
    .select("a.*")

# ─────────────────────────────────────────────
# 5. Clean & Transform
# ─────────────────────────────────────────────
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

# ─────────────────────────────────────────────
# 6. Final column selection
# ─────────────────────────────────────────────
df_final = df_cleaned.select(
    "article_id",
    "title",
    "description",
    "link",
    "pubDate",
    "fetched_at",
    "language",
    "category",
    "country",
    "keywords",
    "creator",
    "datatype",
    "source_id",
    "source_name",
    "source_priority",
    "source_url",
    "image_url",
    "video_url"
)

# ─────────────────────────────────────────────
# 7. PostgreSQL write function
# FIX: added checkpointLocation + error logging
# ─────────────────────────────────────────────
POSTGRES_OPTIONS = {
    "url":      "jdbc:postgresql://localhost:5432/newsdb",
    "dbtable":  "news_articles",
    "user":     "postgres",
    "password": "postgres",
    "driver":   "org.postgresql.Driver"
}

def write_to_postgres(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            print(f"[Batch {batch_id}] Empty batch, skipping.")
            return

        batch_df.write \
            .format("jdbc") \
            .options(**POSTGRES_OPTIONS) \
            .mode("append") \
            .save()

        print(f"[Batch {batch_id}] ✅ Written {batch_df.count()} rows to PostgreSQL")

    except Exception as e:
        print(f"[Batch {batch_id}] ❌ Failed to write: {e}")
        raise e  # re-raise so Spark marks the batch as failed

# ─────────────────────────────────────────────
# 8. Start stream
# FIX: checkpointLocation is required for reliability
# ─────────────────────────────────────────────
query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kafka_news_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()