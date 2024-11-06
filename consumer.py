from utils import start_spark_session
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import sqlite3
from typing import Union
from fastapi import FastAPI
import httpx
import asyncio
from contextlib import asynccontextmanager
import logging


def write_to_sqlite(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:sqlite:flights_db.db") \
        .option("checkpointLocation", "./checkpoints") \
        .option("dbtable", "flights_test") \
        .mode("append") \
        .save()


schema = StructType([
    StructField("id", IntegerType()),
    StructField("airline", StringType()),
    StructField("flight", StringType()),
    StructField("source_city", StringType()),
    StructField("departure_time", StringType()),
    StructField("stops", StringType()),
    StructField("arrival_time", StringType()),
    StructField("destination_city", StringType()),
    StructField("class", StringType()),
    StructField("duration", FloatType()),
    StructField("days_left", IntegerType()),
    StructField("price", FloatType()),


])
def consume():
    spark = start_spark_session("consumer_session")
    test_data = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "flights_test") \
        .option("startingOffsets", "latest") \
        .option("enable.auto.commit", "true") \
        .load() \
        .select(from_json(col("value").cast("string"), schema=schema).alias("parsed_value"))

    test_data = test_data.select("parsed_value.*")

    con = sqlite3.connect('flights_db.db')
    cur = con.cursor()
    limits = httpx.Limits(keepalive_expiry=None)
    with httpx.Client(limits=limits) as client:

        async def process_batch(batch_df, batch_id):

            json_data = batch_df.toJSON().collect()
            # print(json_data)
            response = client.post(
                "http://127.0.0.1:8000/test_model", json=json_data, timeout=None)
            print(
                f"Response from model_api for batch {batch_id}: {response.status_code}, {response.text}")

        writing_sink = test_data.writeStream \
            .foreachBatch(lambda batch_df, batch_id: asyncio.run(process_batch(batch_df, batch_id))) \
            .outputMode("append") \
            .start()
        writing_sink = test_data.writeStream \
            .foreachBatch(write_to_sqlite) \
            .outputMode("append") \
            .start()

        writing_sink.awaitTermination()
    con.commit()
    cur.close()
    con.close()
    spark.stop()



@asynccontextmanager
async def start_consumer(app: FastAPI):
    consume()
    yield

app = FastAPI(title="consumer_api", lifespan=start_consumer)


