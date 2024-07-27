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

    # Print results
    print(f"Data Quality Report for {table_name}:")
    null_counts.show()
    print(f"Total Rows: {total_rows}")
    print(f"Distinct Rows: {distinct_rows}")
    print(f"Duplicate Rows: {total_rows - distinct_rows}")

if __name__ == "__main__":
    spark = get_spark_session()
    check_data_quality(spark, "bank_statements")
    check_data_quality(spark, "credit_reports")
    spark.stop()