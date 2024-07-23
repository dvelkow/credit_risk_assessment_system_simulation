import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from config.config import Config
from data_ingestion import traditional_sources, alternative_sources, public_data
from data_processing import data_quality, data_cleansing, entity_resolution
from data_modeling import dimensional_model, document_store
from analytics import credit_scoring, risk_dashboard, real_time_alerts
from governance import access_control, compliance, data_lineage
from utils.spark_utils import get_spark_session

def generate_mock_data(spark):
    """Generate mock data for testing"""
    
    # Mock bank statement data
    bank_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("balance", FloatType(), True),
        StructField("num_transactions", IntegerType(), True)
    ])
    
    bank_data = [(id, "2023-0" + str(random.randint(1, 9)) + "-01", 
                  random.uniform(100, 10000), random.randint(1, 100))
                 for id in Config.TEST_BUSINESS_IDS for _ in range(10)]
    
    bank_df = spark.createDataFrame(bank_data, bank_schema)
    bank_df.write.parquet(f"{Config.DATA_LAKE_PATH}/bank_statements", mode="overwrite")

    # Mock credit report data
    credit_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("credit_score", IntegerType(), True)
    ])
    
    credit_data = [(id, random.randint(300, 850)) for id in Config.TEST_BUSINESS_IDS]
    credit_df = spark.createDataFrame(credit_data, credit_schema)
    credit_df.write.parquet(f"{Config.DATA_LAKE_PATH}/credit_reports", mode="overwrite")

    print("Mock data generated successfully.")

def main():
    # Initialize Spark session
    spark = get_spark_session()

    # Generate mock data
    generate_mock_data(spark)

    # Data Ingestion
    traditional_sources.ingest_bank_statements(spark, Config.TEST_DATE_RANGE[0], Config.TEST_DATE_RANGE[1])
    traditional_sources.ingest_credit_reports(spark, Config.TEST_BUSINESS_IDS)
    
    if Config.ENABLE_ALTERNATIVE_DATA:
        alternative_sources.ingest_social_media_data(spark, Config.TEST_BUSINESS_IDS)
        alternative_sources.ingest_website_traffic(spark, Config.TEST_BUSINESS_IDS)
    
    if Config.ENABLE_PUBLIC_DATA:
        public_data.ingest_economic_indicators(spark)

    # Data Processing
    data_quality.check_data_quality(spark, "bank_statements")
    data_quality.check_data_quality(spark, "credit_reports")
    data_cleansing.clean_data(spark)
    entity_resolution.resolve_entities(spark)

    # Data Modeling
    dimensional_model.create_dimensional_model(spark)
    document_store.store_unstructured_data(spark)

    # Analytics
    model = credit_scoring.train_credit_scoring_model(spark)
    credit_scoring.score_business(spark, model, Config.TEST_BUSINESS_IDS[0])
    risk_dashboard.update_dashboard(spark)
    
    if Config.ENABLE_REAL_TIME_ALERTS:
        real_time_alerts.check_for_alerts(spark)

    # Governance
    access_control.apply_access_controls()
    compliance.ensure_compliance()
    data_lineage.track_data_lineage()

    spark.stop()

if __name__ == "__main__":
    main()
