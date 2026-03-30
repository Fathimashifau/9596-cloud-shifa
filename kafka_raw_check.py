from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaRawCheck") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value to string
kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

# from pyspark.sql import SparkSession

# # 1️⃣ Create Spark session
# spark = SparkSession.builder \
#     .appName("KafkaRawCheck") \
#     .getOrCreate()

# # 2️⃣ Read streaming data from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "news-data") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # 3️⃣ Convert value from Kafka (binary) to string
# raw_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_string")

# # 4️⃣ Write streaming output to console
# query = raw_df.writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .outputMode("append") \
#     .start()

# # 5️⃣ Keep the stream running
# query.awaitTermination()