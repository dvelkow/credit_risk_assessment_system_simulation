from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from config.config import Config
from utils.spark_utils import get_spark_session

def prepare_data(spark: SparkSession):
    """
    Prepare data for credit scoring by joining different data sources.
    """
    fact_credit_risk = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")
    dim_business = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/dim_business")

    # Join fact table with business dimension
    joined_data = fact_credit_risk.join(dim_business, "business_id")

    # Select relevant features and target variable
    features = ["balance", "num_transactions", "followers", "engagement_rate", "sentiment_score"]
    target = "credit_score"

    return joined_data.select(features + [target])

def train_credit_scoring_model(spark: SparkSession):
    """
    Train a simple credit scoring model using Random Forest Regression.
    """
    # Prepare data
    data = prepare_data(spark)

    # Create feature vector
    assembler = VectorAssembler(inputCols=data.columns[:-1], outputCol="features")
    data_assembled = assembler.transform(data)

    # Split data into training and testing sets
    train_data, test_data = data_assembled.randomSplit([0.8, 0.2], seed=42)

    # Train Random Forest model
    rf = RandomForestRegressor(featuresCol="features", labelCol="credit_score", numTrees=10)
    model = rf.fit(train_data)

    # Make predictions on test data
    predictions = model.transform(test_data)

    # Evaluate model
    evaluator = RegressionEvaluator(labelCol="credit_score", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

    return model

def score_business(spark: SparkSession, model, business_id: str):
    """
    Score a specific business using the trained model.
    """
    # Prepare data for the specific business
    fact_credit_risk = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")
    dim_business = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/dim_business")
    
    business_data = fact_credit_risk.join(dim_business, "business_id").filter(col("business_id") == business_id)

    if business_data.count() == 0:
        print(f"No data found for business ID: {business_id}")
        return

    # Select relevant features
    features = ["balance", "num_transactions", "followers", "engagement_rate", "sentiment_score"]
    data = business_data.select(features)

    # Create feature vector
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    data_assembled = assembler.transform(data)

    # Make prediction
    prediction = model.transform(data_assembled)

    # Extract predicted credit score
    predicted_score = prediction.select("prediction").first()[0]

    print(f"Predicted credit score for business {business_id}: {predicted_score:.2f}")

    # Categorize the credit risk
    risk_category = when(predicted_score >= 700, "Low Risk") \
                    .when((predicted_score >= 650) & (predicted_score < 700), "Moderate Risk") \
                    .when((predicted_score >= 600) & (predicted_score < 650), "Medium Risk") \
                    .when((predicted_score >= 500) & (predicted_score < 600), "High Risk") \
                    .otherwise("Very High Risk")

    print(f"Credit Risk Category: {risk_category}")

def get_risk_assessment(spark: SparkSession, model, business_id: str):
    """
    Get risk assessment for a specific business as a string.
    """
    fact_credit_risk = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/fact_credit_risk")
    dim_business = spark.read.parquet(f"{Config.DATA_LAKE_PATH}/dim_business")
    
    business_data = fact_credit_risk.join(dim_business, "business_id").filter(col("business_id") == business_id)

    if business_data.count() == 0:
        return f"No data found for business ID: {business_id}"

    features = ["balance", "num_transactions", "followers", "engagement_rate", "sentiment_score"]
    data = business_data.select(features)

    assembler = VectorAssembler(inputCols=features, outputCol="features")
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

if __name__ == "__main__":
    spark = get_spark_session()
    model = train_credit_scoring_model(spark)
    score_business(spark, model, Config.TEST_BUSINESS_IDS[0])
    spark.stop()