import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "/tmp/credit_risk_data_lake")
    SPARK_MASTER = "local[*]"
    SPARK_APP_NAME = 'Credit Risk Data Lake'
    TEST_BUSINESS_IDS = ["B001", "B002", "B003"]
    MIN_CREDIT_SCORE = 300
    MAX_CREDIT_SCORE = 850
    MIN_AVG_BALANCE = 1000
    MIN_NUM_TRANSACTIONS = 5
    MIN_SAVINGS_APY = 0.01
    MAX_SAVINGS_APY = 0.05
    MIN_LENDING_RATE = 0.03
    MAX_LENDING_RATE = 0.15
    LOW_RISK_THRESHOLD = 700
    MEDIUM_RISK_THRESHOLD = 600
    HIGH_RISK_THRESHOLD = 500