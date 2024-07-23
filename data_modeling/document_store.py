from pyspark.sql import SparkSession
from config.config import Config
from utils.spark_utils import get_spark_session

def store_unstructured_data(spark: SparkSession):
    """
    Store unstructured data in a Delta table instead of MongoDB.
    """
    # Load unstructured data (e.g., social media posts)
    try:
        social_media_data = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/social_media_data")
        
        # Store the data in a new Delta table
        social_media_data.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/unstructured_data")
        
        print(f"Stored unstructured data in Delta Lake at {Config.DELTA_LAKE_PATH}/unstructured_data")
    except Exception as e:
        print(f"Error storing unstructured data: {str(e)}")

if __name__ == "__main__":
    spark = get_spark_session()
    store_unstructured_data(spark)
    spark.stop()