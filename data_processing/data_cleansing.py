from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from config.config import Config

def clean_data(spark: SparkSession):
    """
    Perform data cleansing operations on the datasets.
    """
    # Read the datasets
    bank_statements = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/bank_statements")
    credit_reports = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/credit_reports")

    # Clean bank statements
    cleaned_bank_statements = bank_statements.withColumn(
        "balance",
        when(col("balance") < 0, 0).otherwise(col("balance"))
    )

    # Clean credit reports
    cleaned_credit_reports = credit_reports.withColumn(
        "credit_score",
        when(col("credit_score") < 300, 300).when(col("credit_score") > 850, 850).otherwise(col("credit_score"))
    )

    # Write cleaned data back to the data lake
    cleaned_bank_statements.write.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_bank_statements", mode="overwrite")
    cleaned_credit_reports.write.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_credit_reports", mode="overwrite")

    print("Data cleaning completed successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataCleansing").getOrCreate()
    clean_data(spark)
    spark.stop()