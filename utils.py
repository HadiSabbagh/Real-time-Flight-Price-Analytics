from pyspark.sql import SparkSession

def start_spark_session(name):
    spark = SparkSession.builder.appName(f"{name}")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.jars", "./jars/sqlite-jdbc-3.47.0.0.jar")\
    .config("spark.driver.extraClassPath", "./jars/sqlite-jdbc-3.47.0.0.jar").getOrCreate()
    return spark

def read_data(spark, filepath):    
    data = spark.read.csv(filepath, header=True, inferSchema=True)
    return data