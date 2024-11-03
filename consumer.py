import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import pandas as pd
import os
from pyspark.sql.functions import split
import sqlite3

schema = StructType([
    StructField("bookId", StringType()),
    StructField("userId", StringType()),
    StructField("rating", FloatType()),
    StructField("timestamp", TimestampType()),
    StructField("title", StringType())
])


spark = SparkSession.builder.appName("demo2").config(
    "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3").config("spark.jars", "./jars/sqlite-jdbc-3.47.0.0.jar").getOrCreate()
# spark.sparkContext.setLogLevel("DEBUG")


con = sqlite3.connect('test.db')

cur = con.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS books (
    bookId TEXT,
    userId TEXT,
    rating REAL,
    timestamp INTEGER,
    title TEXT
)
"""


cur.execute(create_table_query)
con.commit()


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "books") \
    .option("startingOffsets", "latest") \
    .option("enable.auto.commit", "true") \
    .load() \
    .select(from_json(col("value").cast("string"), schema=schema).alias("parsed_value"))


df_ = df.select("parsed_value.*")

# writing_sink = df_.writeStream \
#     .format("json") \
#     .option("path", "./docs/jsonFiles") \
#     .option("checkpointLocation", "./docs/jsonFiles/checkpoint") \
#     .start()


def write_t_sqlite(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:sqlite:test.db") \
        .option("checkpointLocation", "./checkpoints") \
        .option("dbtable", "books") \
        .mode("append") \
        .save()


writing_sink = df_.writeStream \
    .foreachBatch(write_t_sqlite) \
    .outputMode("append") \
    .start()

writing_sink.awaitTermination()
con.commit()
cur.close()
con.close()
