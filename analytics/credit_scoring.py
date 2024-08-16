# analytics/credit_scoring.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from config.config import Config

def calculate_risk_score(credit_score, balance, num_transactions):
    return (credit_score * 0.7 + balance * 0.2 + num_transactions * 0.1) / 10

def calculate_savings_apy(risk_score):
    if risk_score >= Config.LOW_RISK_THRESHOLD:
        return Config.MAX_SAVINGS_APY
    elif risk_score >= Config.MEDIUM_RISK_THRESHOLD:
        return (Config.MAX_SAVINGS_APY + Config.MIN_SAVINGS_APY) / 2
    else:
        return Config.MIN_SAVINGS_APY

def calculate_lending_rate(risk_score):
    if risk_score >= Config.LOW_RISK_THRESHOLD:
        return Config.MIN_LENDING_RATE
    elif risk_score >= Config.MEDIUM_RISK_THRESHOLD:
        return (Config.MIN_LENDING_RATE + Config.MAX_LENDING_RATE) / 2
    else:
        return Config.MAX_LENDING_RATE

def prepare_data(spark):
    return spark.read.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")

def train_credit_scoring_model(spark):
    data = prepare_data(spark)
    
    risk_score_udf = udf(calculate_risk_score, FloatType())
    data = data.withColumn("risk_score", risk_score_udf(col("credit_score"), col("balance"), col("num_transactions")))
    
    feature_columns = ["balance", "num_transactions", "credit_score"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_assembled = assembler.transform(data)

    train_data, test_data = data_assembled.randomSplit([0.8, 0.2], seed=42)

    rf = RandomForestRegressor(featuresCol="features", labelCol="risk_score", numTrees=10)
    model = rf.fit(train_data)

    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="risk_score", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

    return model

def score_business(spark, model, business_id):
    data = prepare_data(spark).filter(col("business_id") == business_id)
    
    if data.count() == 0:
        print(f"No data found for business ID: {business_id}")
        return

    risk_score_udf = udf(calculate_risk_score, FloatType())
    data = data.withColumn("risk_score", risk_score_udf(col("credit_score"), col("balance"), col("num_transactions")))

    savings_apy_udf = udf(calculate_savings_apy, FloatType())
    lending_rate_udf = udf(calculate_lending_rate, FloatType())
    data = data.withColumn("savings_apy", savings_apy_udf(col("risk_score")))
    data = data.withColumn("lending_rate", lending_rate_udf(col("risk_score")))

    feature_columns = ["balance", "num_transactions", "credit_score"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_assembled = assembler.transform(data)

    prediction = model.transform(data_assembled)
    result = prediction.select("business_id", "credit_score", "balance", "num_transactions", "risk_score", "savings_apy", "lending_rate", "prediction").first()

    if result is None:
        print(f"No prediction result for business ID: {business_id}")
        return

    print(f"Credit Risk Assessment for business {business_id}:")
    print(f"Credit Score: {result['credit_score']}")
    print(f"Balance: ${result['balance']:.2f}")
    print(f"Number of Transactions: {result['num_transactions']}")
    print(f"Calculated Risk Score: {result['risk_score']:.2f}")
    print(f"Predicted Risk Score: {result['prediction']:.2f}")
    print(f"Savings APY: {result['savings_apy']:.2%}")
    print(f"Lending Rate: {result['lending_rate']:.2%}")

    risk_category = "Low Risk" if result['risk_score'] >= Config.LOW_RISK_THRESHOLD else \
                    "Medium Risk" if result['risk_score'] >= Config.MEDIUM_RISK_THRESHOLD else \
                    "High Risk"
    print(f"Risk Category: {risk_category}")