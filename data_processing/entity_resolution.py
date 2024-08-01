from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.config import Config

def resolve_entities(spark: SparkSession):
    """
    Perform basic entity resolution to link data from different sources.
    """
    bank_statements = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_bank_statements")
    credit_reports = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_credit_reports")
    
    resolved_data = bank_statements.join(
        credit_reports,
        bank_statements.business_id == credit_reports.business_id,
        "inner"
    ).select(
        bank_statements.business_id,
        bank_statements.transaction_date,
        bank_statements.balance,
        bank_statements.num_transactions,
        credit_reports.credit_score
    )

    resolved_data.write.parquet(f"{Config.DATA_LAKE_PATH}/resolved_entities", mode="overwrite")

    print("Entity resolution completed successfully.")