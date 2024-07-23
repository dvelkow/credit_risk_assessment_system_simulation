from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from config.config import Config
from utils.spark_utils import get_spark_session

def create_dimensional_model(spark: SparkSession):
    """
    Create a simple dimensional model for credit risk analysis.
    """
    # Load resolved entity data
    resolved_data = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/resolved_entities")

    # Create dimension tables
    dim_business = resolved_data.select("business_id", "credit_score").distinct()
    dim_time = resolved_data.select(date_format("transaction_date", "yyyy-MM-dd").alias("date")).distinct()

    # Create fact table
    fact_credit_risk = resolved_data.select(
        "business_id",
        date_format("transaction_date", "yyyy-MM-dd").alias("date"),
        "balance",
        "num_transactions",
        "followers",
        "engagement_rate",
        "sentiment_score"
    )

    # Write dimension and fact tables to Parquet
    dim_business.write.parquet(f"{Config.DATA_LAKE_PATH}/dim_business", mode="overwrite")
    dim_time.write.parquet(f"{Config.DATA_LAKE_PATH}/dim_time", mode="overwrite")
    fact_credit_risk.write.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk", mode="overwrite")

    print("Dimensional model created successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    create_dimensional_model(spark)
    spark.stop()