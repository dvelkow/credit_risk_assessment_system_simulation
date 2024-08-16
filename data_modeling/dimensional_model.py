# data_modeling/dimensional_model.py

from pyspark.sql.functions import col, to_date
from config.config import Config

def create_dimensional_model(spark):
    # Load resolved data
    resolved_data = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/resolved_entities")

    # Create fact table
    fact_credit_risk = resolved_data.select(
        "client_id",
        to_date("latest_transaction_date").alias("date"),
        "latest_balance",
        "total_transactions",
        "latest_credit_score"
    )

    # Create dimension tables
    dim_client = fact_credit_risk.select("client_id").distinct()
    dim_date = fact_credit_risk.select("date").distinct()

    # Write tables
    fact_credit_risk.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")
    dim_client.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/dim_client")
    dim_date.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/dim_date")

    print("Dimensional model created successfully.")