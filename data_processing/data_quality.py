from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from config.config import Config

def check_data_quality(spark: SparkSession, table_name: str):
    """
    Perform basic data quality checks on a Delta table.
    """
    df = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/{table_name}")

    # checking for null values
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    
    # checking for duplicate rows
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    
    # printing the results
    print(f"Data Quality Report for {table_name}:")
    null_counts.show()
    print(f"Total Rows: {total_rows}")
    print(f"Distinct Rows: {distinct_rows}")
    print(f"Duplicate Rows: {total_rows - distinct_rows}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .master(Config.SPARK_MASTER) \
        .getOrCreate()

    check_data_quality(spark, "bank_statements")
    check_data_quality(spark, "credit_reports")

    spark.stop()