from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Constants
MIN_CREDIT_SCORE, MAX_CREDIT_SCORE = 300, 850
MIN_SAVINGS_APY, MAX_SAVINGS_APY = 0.01, 0.05
MIN_LENDING_RATE, MAX_LENDING_RATE = 0.05, 0.25
DATA_LAKE_PATH = "path/to/your/data/lake"

def calculate_risk_score(credit_score, balance, num_transactions):
    credit_factor = (credit_score - MIN_CREDIT_SCORE) / (MAX_CREDIT_SCORE - MIN_CREDIT_SCORE)
    balance_factor = min(balance / 50000, 1)
    transaction_factor = min(num_transactions / 1000, 1)
    
    weighted_score = credit_factor * 0.6 + balance_factor * 0.3 + transaction_factor * 0.1
    return (1 - weighted_score) * 100

def calculate_rates(risk_score):
    savings_apy = max(MIN_SAVINGS_APY, min(MAX_SAVINGS_APY, MIN_SAVINGS_APY + (100 - risk_score) / 20))
    lending_rate = max(MIN_LENDING_RATE, min(MAX_LENDING_RATE, MIN_LENDING_RATE + risk_score / 10))
    return savings_apy, lending_rate

def get_risk_category(risk_score):
    categories = ["Very Low Risk", "Low Risk", "Moderate Risk", "High Risk", "Very High Risk"]
    return categories[min(int(risk_score / 20), 4)]

def prepare_data(spark):
    data = spark.read.parquet(f"{DATA_LAKE_PATH}/fact_credit_risk")
    risk_score_udf = udf(calculate_risk_score, FloatType())
    return data.withColumn("risk_score", risk_score_udf("latest_credit_score", "latest_balance", "total_transactions"))

def train_model(data):
    features = ["latest_balance", "total_transactions", "latest_credit_score"]
    assembled_data = VectorAssembler(inputCols=features, outputCol="features").transform(data)
    train_data, test_data = assembled_data.randomSplit([0.8, 0.2], seed=42)

    model = RandomForestRegressor(featuresCol="features", labelCol="risk_score", numTrees=10).fit(train_data)
    
    predictions = model.transform(test_data)
    rmse = RegressionEvaluator(labelCol="risk_score", predictionCol="prediction", metricName="rmse").evaluate(predictions)
    print(f"Model RMSE: {rmse}")
    
    return model

def score_client(spark, model, client_id):
    client_data = prepare_data(spark).filter(f"client_id = '{client_id}'")
    
    if client_data.count() == 0:
        print(f"No data found for Client ID: {client_id}")
        return

    rates_udf = udf(lambda score: calculate_rates(score), FloatType())
    category_udf = udf(get_risk_category, StringType())
    
    result = model.transform(VectorAssembler(
        inputCols=["latest_balance", "total_transactions", "latest_credit_score"],
        outputCol="features"
    ).transform(client_data))

    result = result.withColumns({
        "savings_apy": rates_udf(result.risk_score)[0],
        "lending_rate": rates_udf(result.risk_score)[1],
        "risk_category": category_udf(result.risk_score)
    }).first()

    print(f"Credit Risk Assessment for Client ID {client_id}:")
    print(f"Credit Score: {result['latest_credit_score']}")
    print(f"Balance: ${result['latest_balance']:.2f}")
    print(f"Total Transactions: {result['total_transactions']}")
    print(f"Calculated Risk Score: {result['risk_score']:.2f}")
    print(f"Predicted Risk Score: {result['prediction']:.2f}")
    print(f"Savings APY: {result['savings_apy']:.2%}")
    print(f"Lending Rate: {result['lending_rate']:.2%}")
    print(f"Risk Category: {result['risk_category']}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CreditScoring").getOrCreate()
    data = prepare_data(spark)
    model = train_model(data)
    score_client(spark, model, "C001")  # Replace with an actual client ID
    spark.stop()