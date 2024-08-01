from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, min, max, stddev
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from config.config import Config

def load_data(spark: SparkSession, path: str):
    try:
        return spark.read.parquet(path)
    except Exception as e:
        print(f"Error loading data from {path}: {str(e)}")
        return None

def calculate_metrics(df):
    return df.agg(
        avg("credit_score").alias("avg_credit_score"),
        min("credit_score").alias("min_credit_score"),
        max("credit_score").alias("max_credit_score"),
        stddev("credit_score").alias("stddev_credit_score"),
        avg("balance").alias("avg_balance"),
        min("balance").alias("min_balance"),
        max("balance").alias("max_balance"),
        stddev("balance").alias("stddev_balance"),
        avg("num_transactions").alias("avg_num_transactions"),
        count("business_id").alias("total_businesses")
    )

def update_dashboard(spark: SparkSession):

    print("Updating risk dashboard...")

    # Load fact table
    fact_credit_risk = load_data(spark, f"{Config.DATA_LAKE_PATH}/fact_credit_risk")
    
    if fact_credit_risk is None:
        print("Failed to update dashboard due to data loading error.")
        return

    metrics = calculate_metrics(fact_credit_risk)

    print("Risk Dashboard Metrics:")
    metrics.show(truncate=False)

    row = metrics.collect()[0]
    
    print(f"Average Credit Score: {row['avg_credit_score']:.2f}")
    print(f"Average Balance: ${row['avg_balance']:.2f}")
    print(f"Total Businesses: {row['total_businesses']}")

    high_risk_businesses = fact_credit_risk.filter(
        (fact_credit_risk.credit_score < Config.MIN_CREDIT_SCORE) |
        (fact_credit_risk.balance < Config.MIN_AVG_BALANCE) |
        (fact_credit_risk.num_transactions < Config.MIN_NUM_TRANSACTIONS)
    ).count()

    print(f"Number of High Risk Businesses: {high_risk_businesses}")
    print(f"Percentage of High Risk Businesses: {(high_risk_businesses / row['total_businesses']) * 100:.2f}%")

    print("Dashboard updated successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("RiskDashboard").getOrCreate()
    update_dashboard(spark)
    spark.stop()