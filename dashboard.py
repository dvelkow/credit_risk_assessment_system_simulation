import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.config import Config
from utils.spark_utils import get_spark_session
from analytics import credit_scoring
import os

def check_data_exists(spark, path):
    try:
        spark.read.parquet(path).limit(1).count()
        return True
    except:
        return False

def load_data(spark):
    fact_path = f"{Config.DATA_LAKE_PATH}/fact_credit_risk"
    dim_path = f"{Config.DATA_LAKE_PATH}/dim_business"
    
    if not check_data_exists(spark, fact_path) or not check_data_exists(spark, dim_path):
        return None

    fact_credit_risk = spark.read.parquet(fact_path)
    dim_business = spark.read.parquet(dim_path)
    return fact_credit_risk.join(dim_business, "business_id")

def main():
    st.title("Credit Risk Dashboard")

    spark = get_spark_session()
    data = load_data(spark)
    
    if data is None:
        st.error("Data not found. Please run the main data pipeline first to generate the necessary data.")
        return

    st.header("Overall Statistics")
    st.write(f"Total number of businesses: {data.count()}")
    
    avg_credit_score = data.agg({"credit_score": "avg"}).collect()[0][0]
    st.write(f"Average credit score: {avg_credit_score:.2f}")

    st.header("Credit Score Distribution")
    credit_score_data = data.select("credit_score").toPandas()
    fig, ax = plt.subplots()
    ax.hist(credit_score_data["credit_score"], bins=20)
    ax.set_xlabel("Credit Score")
    ax.set_ylabel("Count")
    st.pyplot(fig)

    st.header("Business Credit Scores")
    business_data = data.select("business_id", "credit_score").toPandas()
    st.dataframe(business_data)

    st.header("Credit Risk Assessment")
    selected_business = st.selectbox("Select a business ID:", options=business_data["business_id"].tolist())
    if st.button("Assess Credit Risk"):
        model = credit_scoring.train_credit_scoring_model(spark)
        assessment = credit_scoring.get_risk_assessment(spark, model, selected_business)
        st.write(assessment)

    spark.stop()

if __name__ == "__main__":
    main()