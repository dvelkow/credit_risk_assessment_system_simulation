# analytics/risk_dashboard.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, min, max, stddev, col, udf
from pyspark.sql.types import FloatType
from config.config import Config

def calculate_risk_score(credit_score, balance, num_transactions):
    return (credit_score * 0.7 + balance * 0.2 + num_transactions * 0.1) / 10

def calculate_savings_apy(risk_score):
    if risk_score >= Config.LOW_RISK_THRESHOLD:
        return Config.MAX_SAVINGS_APY
    elif risk_score >= Config.MEDIUM_RISK_THRESHOLD:
        return (Config.MAX_SAVINGS_APY + Config.MIN_SAVINGS_APY) / 2
    else:
        return Config.MIN_SAVINGS_APY

def calculate_lending_rate(risk_score):
    if risk_score >= Config.LOW_RISK_THRESHOLD:
        return Config.MIN_LENDING_RATE
    elif risk_score >= Config.MEDIUM_RISK_THRESHOLD:
        return (Config.MIN_LENDING_RATE + Config.MAX_LENDING_RATE) / 2
    else:
        return Config.MAX_LENDING_RATE

def load_data(spark: SparkSession, path: str):
    try:
        return spark.read.parquet(path)
    except Exception as e:
        print(f"Error loading data from {path}: {str(e)}")
        return None

def calculate_metrics(df):
    risk_score_udf = udf(calculate_risk_score, FloatType())
    savings_apy_udf = udf(calculate_savings_apy, FloatType())
    lending_rate_udf = udf(calculate_lending_rate, FloatType())

    df_with_risk = df.withColumn("risk_score", risk_score_udf(col("credit_score"), col("balance"), col("num_transactions")))
    df_with_rates = df_with_risk.withColumn("savings_apy", savings_apy_udf(col("risk_score")))
    df_with_rates = df_with_rates.withColumn("lending_rate", lending_rate_udf(col("risk_score")))

    return df_with_rates.agg(
        avg("credit_score").alias("avg_credit_score"),
        min("credit_score").alias("min_credit_score"),
        max("credit_score").alias("max_credit_score"),
        stddev("credit_score").alias("stddev_credit_score"),
        avg("balance").alias("avg_balance"),
        min("balance").alias("min_balance"),
        max("balance").alias("max_balance"),
        stddev("balance").alias("stddev_balance"),
        avg("num_transactions").alias("avg_num_transactions"),
        avg("risk_score").alias("avg_risk_score"),
        avg("savings_apy").alias("avg_savings_apy"),
        avg("lending_rate").alias("avg_lending_rate"),
        count("business_id").alias("total_businesses")
    )

def update_dashboard(spark: SparkSession):
    print("Updating risk dashboard...")

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
    print(f"Average Risk Score: {row['avg_risk_score']:.2f}")
    print(f"Average Savings APY: {row['avg_savings_apy']:.2%}")
    print(f"Average Lending Rate: {row['avg_lending_rate']:.2%}")
    print(f"Total Businesses: {row['total_businesses']}")

    high_risk_businesses = fact_credit_risk.filter(
        (fact_credit_risk.credit_score < Config.HIGH_RISK_THRESHOLD) |
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