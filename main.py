import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from config.config import Config
from data_processing import data_quality, data_cleansing, entity_resolution
from data_modeling import dimensional_model
from analytics import credit_scoring, risk_dashboard
from utils.spark_utils import get_spark_session

def generate_mock_data(spark):
    # Generate bank statement data
    bank_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("balance", FloatType(), True),
        StructField("num_transactions", IntegerType(), True)
    ])
    bank_data = [
        (id, f"2023-0{random.randint(1, 9)}-01", random.uniform(100, 10000), random.randint(1, 100))
        for id in Config.TEST_BUSINESS_IDS
        for _ in range(10)
    ]
    bank_df = spark.createDataFrame(bank_data, bank_schema)
    bank_df.write.parquet(f"{Config.DATA_LAKE_PATH}/bank_statements", mode="overwrite")

    # Generate credit report data
    credit_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("credit_score", IntegerType(), True)
    ])
    credit_data = [(id, random.randint(300, 850)) for id in Config.TEST_BUSINESS_IDS]
    credit_df = spark.createDataFrame(credit_data, credit_schema)
    credit_df.write.parquet(f"{Config.DATA_LAKE_PATH}/credit_reports", mode="overwrite")
    print("Mock data generated successfully.")

def main():
    spark = get_spark_session()
    generate_mock_data(spark)

    # Data Processing
    data_quality.check_data_quality(spark, "bank_statements")
    data_quality.check_data_quality(spark, "credit_reports")
    data_cleansing.clean_data(spark)
    entity_resolution.resolve_entities(spark)

    # Data Modeling
    dimensional_model.create_dimensional_model(spark)

    # Analytics
    model = credit_scoring.train_credit_scoring_model(spark)
    for business_id in Config.TEST_BUSINESS_IDS:
        credit_scoring.score_business(spark, model, business_id)
    risk_dashboard.update_dashboard(spark)

    spark.stop()

if __name__ == "__main__":
    main()