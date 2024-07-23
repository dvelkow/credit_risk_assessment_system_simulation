from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from config.config import Config
from utils.spark_utils import get_spark_session

def create_dimensional_model(spark: SparkSession):
    """
    Create a simple dimensional model for credit risk analysis.
    """
    # Load resolved entity data
    resolved_data = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/resolved_entities")

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

    # Write dimension and fact tables to Delta Lake
    dim_business.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/dim_business")
    dim_time.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/dim_time")
    fact_credit_risk.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/fact_credit_risk")

    print("Dimensional model created successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    create_dimensional_model(spark)
    spark.stop()