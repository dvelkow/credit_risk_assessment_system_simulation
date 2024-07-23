from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.config import Config

def check_for_alerts(spark: SparkSession):
    """
    Check for any businesses that need immediate attention based on recent data.
    """
    # Load latest credit risk data
    latest_risk_data = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/fact_credit_risk")

    # Define alert conditions
    high_risk_businesses = latest_risk_data.filter(
        (col("credit_score") < Config.MIN_CREDIT_SCORE) | 
        (col("avg_balance") < Config.MIN_AVG_BALANCE) | 
        (col("num_transactions") < Config.MIN_NUM_TRANSACTIONS)
    )

    # In a real-world scenario, you might send these alerts via email or to a monitoring system
    if high_risk_businesses.count() > 0:
        print("ALERT: The following businesses need immediate attention:")
        high_risk_businesses.select("business_id", "credit_score", "avg_balance", "num_transactions").show()
    else:
        print("No high-risk businesses detected.")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .master(Config.SPARK_MASTER) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    check_for_alerts(spark)

    spark.stop()