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
    schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("balance", FloatType(), True),
        StructField("num_transactions", IntegerType(), True),
        StructField("credit_score", IntegerType(), True)
    ])
    
    data = [
        (
            customer_id,
            f"2023-{str(random.randint(1, 12)).zfill(2)}-{str(random.randint(1, 28)).zfill(2)}",
            random.uniform(100, 50000),
            random.randint(1, 100),
            random.randint(Config.MIN_CREDIT_SCORE, Config.MAX_CREDIT_SCORE)
        )
        for customer_id in Config.TEST_CUSTOMER_IDS
        for _ in range(12)  # 12 entries per customer to simulate monthly data
    ]
    
    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(f"{Config.DATA_LAKE_PATH}/personal_financial_data")

    print("Mock data generated successfully.")

def main():
    spark = get_spark_session()
    setup_spark_session(spark)
    
    generate_mock_data(spark)
    create_account_types(spark)

    data_quality.check_data_quality(spark, "personal_financial_data")
    data_cleansing.clean_data(spark)
    entity_resolution.resolve_entities(spark)

    dimensional_model.create_dimensional_model(spark)

    model = credit_scoring.train_credit_scoring_model(spark)
    
    sample_clients = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/dim_client") \
                             .limit(10) \
                             .select("client_id") \
                             .rdd.flatMap(lambda x: x).collect()
    
    for client_id in sample_clients:
        try:
            credit_scoring.score_client(spark, model, client_id)
        except Exception as e:
            print(f"Error scoring client {client_id}: {str(e)}")
    
    risk_dashboard.update_dashboard(spark)

    spark.stop()

if __name__ == "__main__":
    main()