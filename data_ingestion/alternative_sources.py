import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from config.config import Config
from utils.spark_utils import get_spark_session

def ingest_social_media_data(spark: SparkSession, business_ids: list):
    """
    Generate and ingest mock social media data for given business IDs.
    """
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("followers", IntegerType(), True),
        StructField("engagement_rate", FloatType(), True),
        StructField("sentiment_score", FloatType(), True)
    ])
    
    data = [(id, random.randint(100, 10000), random.uniform(0.01, 0.1), random.uniform(-1, 1))
            for id in business_ids]
    
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/social_media_data")
    print("Social media data ingested successfully.")

def ingest_website_traffic(spark: SparkSession, business_ids: list):
    """
    Generate and ingest mock website traffic data for given business IDs.
    """
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("monthly_visits", IntegerType(), True),
        StructField("avg_time_on_site", FloatType(), True),
        StructField("bounce_rate", FloatType(), True)
    ])
    
    data = [(id, random.randint(1000, 100000), random.uniform(1, 10), random.uniform(0.1, 0.9))
            for id in business_ids]
    
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/website_traffic")
    print("Website traffic data ingested successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    ingest_social_media_data(spark, Config.TEST_BUSINESS_IDS)
    ingest_website_traffic(spark, Config.TEST_BUSINESS_IDS)
    spark.stop()
