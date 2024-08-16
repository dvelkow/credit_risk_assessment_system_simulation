import requests
from pyspark.sql import SparkSession
from config.config import Config

def ingest_bank_statements(spark: SparkSession, start_date: str, end_date: str):
    """
    Ingest bank statements from the API and save as a Delta table.
    """
    response = requests.get(
        f"{Config.BANK_STATEMENT_API}",
        params={"start_date": start_date, "end_date": end_date}
    )
    data = response.json()

    # Create DataFrame
    df = spark.createDataFrame(data)

    # Write to Delta Lake
    df.write.format("delta").mode("append").save(f"{Config.DELTA_LAKE_PATH}/bank_statements")

def ingest_credit_reports(spark: SparkSession, business_ids: list):
    """
    Ingest credit reports for given business IDs and save as a Delta table.
    """
    credit_data = []
    for business_id in business_ids:
        response = requests.get(f"{Config.CREDIT_BUREAU_API}/{business_id}")
        credit_data.append(response.json())

    df = spark.createDataFrame(credit_data)
    df.write.format("delta").mode("append").save(f"{Config.DELTA_LAKE_PATH}/credit_reports")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .master(Config.SPARK_MASTER) \
        .getOrCreate()

    ingest_bank_statements(spark, "2023-01-01", "2023-12-31")
    ingest_credit_reports(spark, ["B001", "B002", "B003"])

    spark.stop()