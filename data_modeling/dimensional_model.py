from pyspark.sql.functions import col, date_format
from config.config import Config

def create_dimensional_model(spark):
    bank_statements = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_bank_statements")
    credit_reports = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_credit_reports")
    account_types = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/account_types")

    fact_credit_risk = bank_statements.join(
        credit_reports, "business_id"
    ).select(
        "business_id",
        date_format("transaction_date", "yyyy-MM-dd").alias("date"),
        "balance",
        "num_transactions",
        "credit_score"
    )

    dim_business = fact_credit_risk.select("business_id").distinct()
    dim_date = fact_credit_risk.select("date").distinct()
    dim_account_type = account_types

    fact_credit_risk.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")
    dim_business.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/dim_business")
    dim_date.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/dim_date")
    dim_account_type.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/dim_account_type")

    print("Dimensional model created successfully.")