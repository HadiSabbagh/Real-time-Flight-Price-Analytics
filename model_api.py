
from utils import read_data, start_spark_session
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import RandomForestRegressionModel
from preprocess_class import pre_process
from utils import start_spark_session
from pyspark.sql.types import DoubleType
from preprocess_class import pre_process
from utils import read_data
from typing import Union
from fastapi import FastAPI, Response, status

import json
from contextlib import asynccontextmanager
import logging

global spark
global fitted_pipeline_model
global random_forest_model

@asynccontextmanager
async def lifespan_prep(app: FastAPI):
    global spark
    global fitted_pipeline_model
    global random_forest_model

    # logging.getLogger().setLevel(logging.INFO)
    print("Starting session")
    try:
        spark = start_spark_session("api_session")
        
        logging.info("Session started")
    except:
        logging.error("Session failed to start")
        
    logging.info("Loading model")
    try:
        fitted_pipeline_model = PipelineModel.load("./docs/pipeline_model_backup")
        random_forest_model = RandomForestRegressionModel.load("./docs/random_forest_model")
        logging.info("Loaded model")
    except:
        logging.error("Failed to load model")
    yield
    
    
app = FastAPI(title="model_api",lifespan=lifespan_prep)


@app.post("/test_model")
def test_model(test_data: list[str], response:Response):
    try:
        parsed_data = [json.loads(record) for record in test_data]
        df = spark.createDataFrame(parsed_data)

        
        prepared_data = fitted_pipeline_model.transform(
            df).select("features", "price")

        # lr = LinearRegression(featuresCol="features",labelCol="price")
        # lrModel = lr.fit(prepared_data)
        # predictions = lrModel.transform(prepared_data)

        # evaluator = RegressionEvaluator(labelCol="price", predictionCol='prediction', metricName='r2')
        # lr_r2 = evaluator.evaluate(predictions)

        

        predictions = random_forest_model.transform(prepared_data)
        
        evaluator = RegressionEvaluator(
            labelCol="price", predictionCol='prediction', metricName='mae')
        rf_mae = evaluator.evaluate(predictions)
        print("random_forest_mae:", rf_mae)
        predictions.select("prediction", "price").show()
        response.status_code = status.HTTP_200_OK
        return "Successfully processed batch"
    except:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return "Batch could not be processed"
    # return f"random_forest_r2:{rf_r2}"

    # print("RandomForestRegressor R2 score:", rf_r2)
    # return {"linear_regression_r2": lr_r2, "random_forest_r2": rf_r2}



