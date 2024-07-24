
from pyspark.sql.functions import col, date_format
from config.config import Config

def create_dimensional_model(spark):
    # Load cleaned data
    bank_statements = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_bank_statements")
    credit_reports = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_credit_reports")

    # Create fact table
    fact_credit_risk = bank_statements.join(
        credit_reports, "business_id"
    ).select(
        "business_id",
        date_format("transaction_date", "yyyy-MM-dd").alias("date"),
        "balance",
        "num_transactions",
        "credit_score"
    )

    # Create dimension tables
    dim_business = fact_credit_risk.select("business_id").distinct()
    dim_date = fact_credit_risk.select("date").distinct()

    # Write tables
    fact_credit_risk.write.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk", mode="overwrite")
    dim_business.write.parquet(f"{Config.DATA_LAKE_PATH}/dim_business", mode="overwrite")
    dim_date.write.parquet(f"{Config.DATA_LAKE_PATH}/dim_date", mode="overwrite")

    print("Dimensional model created successfully.")
