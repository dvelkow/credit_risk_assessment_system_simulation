from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from config.config import Config

def prepare_data(spark):
    return spark.read.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")

def train_credit_scoring_model(spark):
    data = prepare_data(spark)
    
    feature_columns = ["balance", "num_transactions"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_assembled = assembler.transform(data)

    train_data, test_data = data_assembled.randomSplit([0.8, 0.2], seed=42)

    rf = RandomForestRegressor(featuresCol="features", labelCol="credit_score", numTrees=10)
    model = rf.fit(train_data)

    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="credit_score", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

    return model

def score_business(spark, model, business_id):
    data = prepare_data(spark).filter(col("business_id") == business_id)
    
    if data.count() == 0:
        print(f"No data found for business ID: {business_id}")
        return

    feature_columns = ["balance", "num_transactions"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_assembled = assembler.transform(data)

    prediction = model.transform(data_assembled)
    predicted_score = prediction.select("prediction").first()[0]

    print(f"Predicted credit score for business {business_id}: {predicted_score:.2f}")

    risk_category = "Very High Risk"
    if predicted_score >= 700:
        risk_category = "Low Risk"
    elif predicted_score >= 650:
        risk_category = "Moderate Risk"
    elif predicted_score >= 600:
        risk_category = "Medium Risk"
    elif predicted_score >= 500:
        risk_category = "High Risk"

    print(f"Credit Risk Category: {risk_category}")

def get_risk_assessment(spark, model, business_id):
    data = prepare_data(spark).filter(col("business_id") == business_id)
    
    if data.count() == 0:
        return f"No data found for business ID: {business_id}"

    feature_columns = ["balance", "num_transactions"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_assembled = assembler.transform(data)

    prediction = model.transform(data_assembled)
    predicted_score = prediction.select("prediction").first()[0]

    risk_category = "Very High Risk"
    if predicted_score >= 700:
        risk_category = "Low Risk"
    elif predicted_score >= 650:
        risk_category = "Moderate Risk"
    elif predicted_score >= 600:
        risk_category = "Medium Risk"
    elif predicted_score >= 500:
        risk_category = "High Risk"

    return f"Predicted credit score for business {business_id}: {predicted_score:.2f}\nCredit Risk Category: {risk_category}"