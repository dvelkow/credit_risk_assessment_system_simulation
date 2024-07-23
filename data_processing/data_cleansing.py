from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from config.config import Config
from utils.spark_utils import get_spark_session

def clean_data(spark: SparkSession):
    """
    Perform data cleansing operations on the ingested data.
    """
    # Load data from Delta tables
    bank_statements = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/bank_statements")
    credit_reports = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/credit_reports")

    # Clean bank statements
    cleaned_bank_statements = bank_statements.na.fill(0) \
        .withColumn("balance", when(col("balance") < 0, 0).otherwise(col("balance")))

    # Clean credit reports
    cleaned_credit_reports = credit_reports.na.drop() \
        .withColumn("credit_score", when(col("credit_score") > 850, 850).otherwise(col("credit_score")))

    # Write cleaned data back to Delta Lake
    cleaned_bank_statements.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/cleaned_bank_statements")
    cleaned_credit_reports.write.format("delta").mode("overwrite").save(f"{Config.DELTA_LAKE_PATH}/cleaned_credit_reports")

    print("Data cleaning completed successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    clean_data(spark)
    spark.stop()