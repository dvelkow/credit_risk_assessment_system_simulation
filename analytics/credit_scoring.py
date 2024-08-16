from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from config.config import Config

def calculate_risk_score(credit_score, balance, num_transactions):
    # Normalize each factor
    credit_score_factor = (credit_score - Config.MIN_CREDIT_SCORE) / (Config.MAX_CREDIT_SCORE - Config.MIN_CREDIT_SCORE)
    balance_factor = min(balance / 50000, 1)  # Assuming 50,000 as a high balance
    transaction_factor = min(num_transactions / 1000, 1)  # Assuming 1000 as a high number of transactions
    
    # Calculate weighted score (higher is better)
    weighted_score = credit_score_factor * 0.6 + balance_factor * 0.3 + transaction_factor * 0.1
    
    # Convert to a 0-100 scale where lower is riskier
    return (1 - weighted_score) * 100

def calculate_savings_apy(risk_score):
    return max(Config.MIN_SAVINGS_APY, min(Config.MAX_SAVINGS_APY, Config.MIN_SAVINGS_APY + (100 - risk_score) / 20))

def calculate_lending_rate(risk_score):
    return max(Config.MIN_LENDING_RATE, min(Config.MAX_LENDING_RATE, Config.MIN_LENDING_RATE + risk_score / 10))

def determine_risk_category(risk_score):
    if risk_score < 20:
        return "Very Low Risk"
    elif risk_score < 40:
        return "Low Risk"
    elif risk_score < 60:
        return "Moderate Risk"
    elif risk_score < 80:
        return "High Risk"
    else:
        return "Very High Risk"

def prepare_data(spark):
    return spark.read.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")

def train_credit_scoring_model(spark):
    data = prepare_data(spark)
    
    risk_score_udf = udf(calculate_risk_score, FloatType())
    data = data.withColumn("risk_score", risk_score_udf(col("latest_credit_score"), col("latest_balance"), col("total_transactions")))
    
    feature_columns = ["latest_balance", "total_transactions", "latest_credit_score"]
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

def score_client(spark, model, client_id):
    data = prepare_data(spark).filter(col("client_id") == client_id)
    
    if data.count() == 0:
        print(f"No data found for Client ID: {client_id}")
        return

    risk_score_udf = udf(calculate_risk_score, FloatType())
    data = data.withColumn("risk_score", risk_score_udf(col("latest_credit_score"), col("latest_balance"), col("total_transactions")))

    savings_apy_udf = udf(calculate_savings_apy, FloatType())
    lending_rate_udf = udf(calculate_lending_rate, FloatType())
    risk_category_udf = udf(determine_risk_category, StringType())
    
    data = data.withColumn("savings_apy", savings_apy_udf(col("risk_score")))
    data = data.withColumn("lending_rate", lending_rate_udf(col("risk_score")))
    data = data.withColumn("risk_category", risk_category_udf(col("risk_score")))

    feature_columns = ["latest_balance", "total_transactions", "latest_credit_score"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_assembled = assembler.transform(data)

    prediction = model.transform(data_assembled)
    result = prediction.select("client_id", "latest_credit_score", "latest_balance", "total_transactions", "risk_score", "savings_apy", "lending_rate", "risk_category", "prediction").first()

    print(f"Credit Risk Assessment for Client ID {client_id}:")
    print(f"Credit Score: {result['latest_credit_score']}")
    print(f"Balance: ${result['latest_balance']:.2f}")
    print(f"Total Transactions: {result['total_transactions']}")
    print(f"Calculated Risk Score: {result['risk_score']:.2f}")
    print(f"Predicted Risk Score: {result['prediction']:.2f}")
    print(f"Savings APY: {result['savings_apy']:.2%}")
    print(f"Lending Rate: {result['lending_rate']:.2%}")
    print(f"Risk Category: {result['risk_category']}")
    print(f"")
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("CreditScoring").getOrCreate()
    model = train_credit_scoring_model(spark)
    score_client(spark, model, "C001")  # Replace with an actual client ID
    spark.stop()