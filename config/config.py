import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Storage configurations
    DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "/tmp/credit_risk_data_lake")

    # Spark configurations
    SPARK_MASTER = "local[*]"
    SPARK_APP_NAME = 'Credit Risk Data Lake'

    # Test data
    TEST_BUSINESS_IDS = ["B001", "B002", "B003"]
    TEST_DATE_RANGE = ("2023-01-01", "2023-12-31")

    # Feature flags
    ENABLE_ALTERNATIVE_DATA = True
    ENABLE_PUBLIC_DATA = True
    ENABLE_REAL_TIME_ALERTS = True

    # Thresholds for credit risk scoring
    MIN_CREDIT_SCORE = 500
    MIN_AVG_BALANCE = 1000
    MIN_NUM_TRANSACTIONS = 5
