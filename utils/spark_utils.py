from pyspark.sql import SparkSession
from config.config import Config

def get_spark_session():
    return SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .master(Config.SPARK_MASTER) \
        .getOrCreate()
