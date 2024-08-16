from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from config.config import Config

def clean_data(spark: SparkSession):
    """
    Perform data cleansing operations on the datasets.
    """
    # Read the dataset
    personal_financial_data = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/personal_financial_data")

    # Clean personal financial data
    cleaned_data = personal_financial_data.withColumn(
        "balance",
        when(col("balance") < 0, 0).otherwise(col("balance"))
    ).withColumn(
        "credit_score",
        when(col("credit_score") < 300, 300).when(col("credit_score") > 850, 850).otherwise(col("credit_score"))
    ).withColumn(
        "num_transactions",
        when(col("num_transactions") < 0, 0).otherwise(col("num_transactions"))
    )

    # Write cleaned data back to the data lake
    cleaned_data.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/cleaned_personal_financial_data")

    print("Data cleaning completed successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataCleansing").getOrCreate()
    clean_data(spark)
    spark.stop()