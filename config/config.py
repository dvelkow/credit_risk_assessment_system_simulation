import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

class Config:
    # Data sources (using mock APIs for testing)
    BANK_STATEMENT_API = "https://mockapi.com/bank_statements"
    CREDIT_BUREAU_API = "https://mockapi.com/credit_reports"
    SOCIAL_MEDIA_API = "https://mockapi.com/social_media"
    ECONOMIC_DATA_API = "https://mockapi.com/economic_indicators"
    WEBSITE_TRAFFIC_API = "https://mockapi.com/website_traffic"

    # Storage configurations (using local paths for testing)
    DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "/tmp/credit_risk_data_lake")

    # Spark configurations
    SPARK_MASTER = "local[*]"  # Run Spark locally
    SPARK_APP_NAME = 'Credit Risk Data Lake'

    # Logging
    LOG_LEVEL = 'INFO'
    LOG_FILE = 'credit_risk_data_lake.log'

    # Test data
    TEST_BUSINESS_IDS = ["B001", "B002", "B003"]
    TEST_DATE_RANGE = ("2023-01-01", "2023-12-31")

    # Feature flags for enabling/disabling components
    ENABLE_ALTERNATIVE_DATA = True
    ENABLE_PUBLIC_DATA = True
    ENABLE_REAL_TIME_ALERTS = True

    # Thresholds for credit risk scoring
    MIN_CREDIT_SCORE = 500
    MIN_AVG_BALANCE = 1000
    MIN_NUM_TRANSACTIONS = 5