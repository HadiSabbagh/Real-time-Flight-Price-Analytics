from utils import start_spark_session
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import json
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressionModel

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

    writing_sink = test_data.writeStream \
        .foreachBatch(lambda batch_df, batch_id: test_model(batch_df, batch_id, spark)) \
        .outputMode("append") \
        .start()

    writing_sink.awaitTermination()


def test_model(test_data, batch_id, spark):
    test_data = test_data.toJSON().collect()
    try:
        fitted_pipeline_model = PipelineModel.load(
            "./docs/pipeline_model_backup")
        random_forest_model = RandomForestRegressionModel.load(
            "./docs/random_forest_model")

        parsed_data = [json.loads(record) for record in test_data]
        df = spark.createDataFrame(parsed_data)

        prepared_data = fitted_pipeline_model.transform(
            df).select("features", "price")

        predictions = random_forest_model.transform(prepared_data)

        r2_evaluator = RegressionEvaluator(
            labelCol="price", predictionCol="prediction", metricName="r2")
        rmse_evaluator = RegressionEvaluator(
            labelCol="price", predictionCol="prediction", metricName="rmse")
        mae_evaluator = RegressionEvaluator(
            labelCol="price", predictionCol="prediction", metricName="mae")

        r2 = r2_evaluator.evaluate(predictions)
        rmse = rmse_evaluator.evaluate(predictions)
        mae = mae_evaluator.evaluate(predictions)

        print("R²:", r2)
        print("RMSE:", rmse)
        print("MAE:", mae)
        predictions.select("prediction", "price").show()
        return "Successfully processed batch"
    except:
        return "Batch could not be processed"


if __name__ == "__main__":
    consume()
