from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
from config.config import Config

def update_dashboard(spark: SparkSession):
    """
    Update the risk dashboard with latest metrics.
    """
    # Load fact table
    fact_credit_risk = spark.read.format("delta").load(f"{Config.DELTA_LAKE_PATH}/fact_credit_risk")

    # Calculate key metrics
    metrics = fact_credit_risk.agg(
        avg("credit_score").alias("avg_credit_score"),
        avg("avg_balance").alias("avg_balance"),
        count("business_id").alias("total_businesses")
    )

    # In a real-world scenario, you might save these metrics to a database or send them to a frontend
    metrics.show()

    print("Dashboard updated successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .master(Config.SPARK_MASTER) \
        .getOrCreate()

    update_dashboard(spark)

    spark.stop()