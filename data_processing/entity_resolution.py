from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, concat_ws
from config.config import Config
from utils.spark_utils import get_spark_session

def resolve_entities(spark: SparkSession):
    """
    Perform basic entity resolution to link data from different sources.
    """
    # Load data from Delta tables
    bank_statements = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/cleaned_bank_statements")
    credit_reports = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/cleaned_credit_reports")
    social_media_data = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/social_media_data")

    # Perform entity resolution (simplified example)
    resolved_data = bank_statements.join(
        credit_reports,
        bank_statements.business_id == credit_reports.business_id,
        "inner"
    ).join(
        social_media_data,
        bank_statements.business_id == social_media_data.business_id,
        "left_outer"
    )

    # Write resolved data to Delta Lake
    resolved_data.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/resolved_entities")

    print("Entity resolution completed successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    resolve_entities(spark)
    spark.stop()