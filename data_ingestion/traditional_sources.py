from pyspark.sql import SparkSession
from config.config import Config
from utils.spark_utils import get_spark_session

def ingest_bank_statements(spark: SparkSession, start_date: str, end_date: str):
    """
    Ingest bank statements from the mock data.
    """
    df = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/bank_statements")
    df.filter(f"transaction_date >= '{start_date}' AND transaction_date <= '{end_date}'") \
      .write.parquet(f"{Config.DATA_LAKE_PATH}/ingested_bank_statements", mode="overwrite")
    print("Bank statements ingested successfully.")

def ingest_credit_reports(spark: SparkSession, business_ids: list):
    """
    Ingest credit reports for given business IDs from the mock data.
    """
    df = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/credit_reports")
    df.filter(df.business_id.isin(business_ids)) \
      .write.parquet(f"{Config.DATA_LAKE_PATH}/ingested_credit_reports", mode="overwrite")
    print("Credit reports ingested successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    ingest_bank_statements(spark, Config.TEST_DATE_RANGE[0], Config.TEST_DATE_RANGE[1])
    ingest_credit_reports(spark, Config.TEST_BUSINESS_IDS)
    spark.stop()
