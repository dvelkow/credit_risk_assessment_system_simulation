from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from config.config import Config

def create_account_types(spark: SparkSession):
    schema = StructType([
        StructField("account_type", StringType(), False),
        StructField("min_balance", FloatType(), False),
        StructField("base_apy", FloatType(), False)
    ])
    
    data = [
        ("Basic Savings", 0.0, 0.01),
        ("Premium Savings", 10000.0, 0.02),
        ("Basic Lending", 0.0, 0.05),
        ("Premium Lending", 50000.0, 0.04)
    ]
    
    account_types_df = spark.createDataFrame(data, schema)
    account_types_df.repartition(10).write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/account_types")
    
    print("Account types data created successfully.")
