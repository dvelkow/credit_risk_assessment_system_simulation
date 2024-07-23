import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from config.config import Config
from utils.spark_utils import get_spark_session

def ingest_economic_indicators(spark: SparkSession):
    """
    Generate and ingest mock economic indicators data.
    """
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("gdp_growth", FloatType(), True),
        StructField("unemployment_rate", FloatType(), True),
        StructField("inflation_rate", FloatType(), True)
    ])

    data = [
        ("2023-" + str(month).zfill(2) + "-01", 
         random.uniform(-1, 5), 
         random.uniform(3, 10), 
         random.uniform(0, 5))
        for month in range(1, 13)
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/economic_indicators")
    print("Economic indicators data ingested successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    ingest_economic_indicators(spark)
    spark.stop()