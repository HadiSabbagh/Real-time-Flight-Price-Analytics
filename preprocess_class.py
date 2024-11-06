import numpy as np
import pandas as pd
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor


class pre_process:



    def __init__(self, train_data):
        self.train_data = train_data
        self.train_numerical_features = ['duration', 'days_left']
        self.train_categorical_features = self.get_categorical_features()

    def get_categorical_features(self):
        pandas_df = self.train_data.toPandas()
        categorical_features = pandas_df.select_dtypes(
            include='object').columns.tolist()
        return categorical_features

    # Convert categorical features into nominal features

    def get_indexers(self):
        indexers = [StringIndexer(inputCol=feature, outputCol=feature + "_index",
                                  handleInvalid='keep') for feature in self.train_categorical_features]
        return indexers

    # One hot encode categorical features
    def get_encoders(self):
        encoders = [OneHotEncoder(inputCols=[feature + "_index"], outputCols=[feature + "_encoded"],
                                  handleInvalid='keep') for feature in self.train_categorical_features]
        return encoders

    # Scale numerical features
    def get_numerical_f_assembler_scaler(self):
        numerical_assembler = VectorAssembler(
            inputCols=self.train_numerical_features, outputCol="numerical_features")
        scaler = MinMaxScaler(inputCol="numerical_features",
                              outputCol="numerical_features_scaled")
        return numerical_assembler, scaler

    def get_pipeline_model(self):

        indexers = self.get_indexers()
        encoders = self.get_encoders()
        numerical_assembler, scaler = self.get_numerical_f_assembler_scaler()
        encoded_feature_cols = [
            feature + "_encoded" for feature in self.train_categorical_features]
        
        final_assembler = VectorAssembler(
            inputCols=encoded_feature_cols + ["numerical_features_scaled"],
            outputCol="features"
        )


        pipeline_indexers_encoders = Pipeline(
            stages=indexers + encoders + [numerical_assembler, scaler, final_assembler])
        pipeline_model = pipeline_indexers_encoders.fit(self.train_data)

       

        return pipeline_model

    def save_fitted_pipeline_model(self, pipeline_model, path_string):
        try:
            pipeline_model.save(path_string)
        except:
            print("Error Saving Pipeline Model")

    # Assemble vectorized features (categorical and numerical)
    def get_final_assembler(self):

        self.final_assembler = final_assembler
