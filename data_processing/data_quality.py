from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from config.config import Config

def check_data_quality(spark: SparkSession, table_name: str):
    """
    Perform basic data quality checks on a Parquet table.
    """
    df = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/{table_name}")

    # Check for null values
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])

    # Check for duplicate rows
    total_rows = df.count()
    distinct_rows = df.distinct().count()


    # Additional checks for personal financial data
    if table_name == "personal_financial_data":
        print("\nAdditional checks for personal financial data:")
        print("Credit score range:")
        df.agg({"credit_score": "min"}).show()
        df.agg({"credit_score": "max"}).show()
        
        print("Balance range:")
        df.agg({"balance": "min"}).show()
        df.agg({"balance": "max"}).show()
        
        print("Number of transactions range:")
        df.agg({"num_transactions": "min"}).show()
        df.agg({"num_transactions": "max"}).show()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    check_data_quality(spark, "personal_financial_data")
    spark.stop()