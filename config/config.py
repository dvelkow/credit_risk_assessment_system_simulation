# config/config.py

import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

class Config:
    # data sources
    BANK_STATEMENT_API = os.getenv('BANK_STATEMENT_API')
    CREDIT_BUREAU_API = os.getenv('CREDIT_BUREAU_API')
    SOCIAL_MEDIA_API = os.getenv('SOCIAL_MEDIA_API')
    ECONOMIC_DATA_API = os.getenv('ECONOMIC_DATA_API')

    # storage configurations
    HDFS_NAMENODE = os.getenv('HDFS_NAMENODE')
    DELTA_LAKE_PATH = os.getenv('DELTA_LAKE_PATH')

    # spark configurations
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
    SPARK_APP_NAME = 'Credit Risk Data Lake'

    # MongoDB configurations
    MONGODB_URI = os.getenv('MONGODB_URI')

    # logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'credit_risk_data_lake.log')