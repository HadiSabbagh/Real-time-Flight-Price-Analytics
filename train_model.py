from utils import start_spark_session
from preprocess_class import pre_process
from utils import read_data
from pyspark.ml.regression import RandomForestRegressor

def train_model():
    
    spark = start_spark_session("train_model")
    train_data = read_data(spark,"./docs/train.csv")
    
    pre_processor = pre_process(train_data)
    pipeline_model = pre_processor.get_pipeline_model()
    prepared_data = pipeline_model.transform(train_data)
    pre_processor.save_fitted_pipeline_model(pipeline_model, "./docs/pipeline_model_backup")
    random_forest = RandomForestRegressor(
            featuresCol="features", labelCol="price")
    random_forest_model = random_forest.fit(prepared_data)
    random_forest_model.save("./docs/random_forest_model")
    
    spark.stop()

if __name__ == "__main__":
    train_model()

