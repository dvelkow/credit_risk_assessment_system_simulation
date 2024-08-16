# data_processing/entity_resolution.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, sum, last
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from config.config import Config

def resolve_entities(spark: SparkSession):
    """
    Perform entity resolution to link and aggregate data for each client.
    """
    cleaned_data = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/cleaned_personal_financial_data")
    
    # Create a window spec to get the latest record for each client
    window_spec = Window.partitionBy("client_id").orderBy(col("transaction_date").desc())
    
    # Add a row number column to identify the latest record for each client
    data_with_row_number = cleaned_data.withColumn("row_number", row_number().over(window_spec))
    
    # Aggregate data and get the latest record for each client
    resolved_data = data_with_row_number.groupBy("client_id").agg(
        max("transaction_date").alias("latest_transaction_date"),
        last("balance").alias("latest_balance"),
        sum("num_transactions").alias("total_transactions"),
        last("credit_score").alias("latest_credit_score")
    )

    resolved_data.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/resolved_entities")

    print("Entity resolution completed successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("EntityResolution").getOrCreate()
    resolve_entities(spark)
    spark.stop()