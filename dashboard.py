import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from config.config import Config
from utils.spark_utils import get_spark_session
from analytics import credit_scoring

def load_data(spark):
    return spark.read.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")

def main():
    st.title("Credit Risk Dashboard")

    spark = get_spark_session()
    data = load_data(spark)
    
    if data.count() == 0:
        st.error("No data found. Please run the main data pipeline first to generate the necessary data.")
        return

    st.header("Overall Statistics")
    st.write(f"Total number of businesses: {data.select('business_id').distinct().count()}")
    
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
    business_data = data.groupBy("business_id").agg({"credit_score": "avg"}).toPandas()
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