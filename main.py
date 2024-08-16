import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from config.config import Config
from data_processing import data_quality, data_cleansing, entity_resolution
from data_modeling import dimensional_model
from analytics import credit_scoring, risk_dashboard
from data_ingestion.account_types import create_account_types
from utils.spark_utils import get_spark_session, setup_spark_session

def generate_mock_data(spark):
    # Generating bank statement data
    bank_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("balance", FloatType(), True),
        StructField("num_transactions", IntegerType(), True)
    ])
    bank_data = [
        (f"B{str(id).zfill(3)}", f"2023-{str(month).zfill(2)}-{str(day).zfill(2)}", random.uniform(100, 10000), random.randint(1, 100))
        for id in range(1, 101)  # 100 businesses
        for month in range(1, 13)  # Data for each month
        for day in range(1, 29)  # Data for each day (simplified to 28 days per month)
    ]
    bank_df = spark.createDataFrame(bank_data, bank_schema)
    bank_df.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/bank_statements")

    # Generating random credit report data
    credit_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("credit_score", IntegerType(), True)
    ])
    credit_data = [(f"B{str(id).zfill(3)}", random.randint(300, 850)) for id in range(1, 101)]
    credit_df = spark.createDataFrame(credit_data, credit_schema)
    credit_df.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/credit_reports")

    print("Mock data generated successfully.")

def main():
    spark = get_spark_session()
    setup_spark_session(spark)
    
    generate_mock_data(spark)
    create_account_types(spark)

    data_quality.check_data_quality(spark, "bank_statements")
    data_quality.check_data_quality(spark, "credit_reports")
    data_cleansing.clean_data(spark)
    entity_resolution.resolve_entities(spark)

    dimensional_model.create_dimensional_model(spark)

    model = credit_scoring.train_credit_scoring_model(spark)
    
    # Score a sample of businesses
    sample_businesses = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/dim_business") \
                             .limit(10) \
                             .select("business_id") \
                             .rdd.flatMap(lambda x: x).collect()
    
    for business_id in sample_businesses:
        credit_scoring.score_business(spark, model, business_id)
    
    risk_dashboard.update_dashboard(spark)

    spark.stop()

if __name__ == "__main__":
    main()