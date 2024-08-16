# utils/spark_utils.py

from pyspark.sql import SparkSession
from config.config import Config

def get_spark_session():
    return (SparkSession.builder
        .appName(Config.SPARK_APP_NAME)
        .master(Config.SPARK_MASTER)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.default.parallelism", "100")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED")
        .getOrCreate())

def setup_spark_session(spark):
    spark.sparkContext.setLogLevel("ERROR")

def read_parquet(spark, path):
    """
    Read a Parquet file and return a DataFrame.
    """
    try:
        return spark.read.parquet(path)
    except Exception as e:
        print(f"Error reading Parquet file from {path}: {str(e)}")
        return None

def write_parquet(df, path, mode="overwrite"):
    """
    Write a DataFrame to a Parquet file.
    """
    try:
        df.write.mode(mode).parquet(path)
        print(f"Successfully wrote DataFrame to {path}")
    except Exception as e:
        print(f"Error writing DataFrame to {path}: {str(e)}")

def count_rows(df):
    """
    Count the number of rows in a DataFrame.
    """
    return df.count()

def show_sample(df, n=5):
    """
    Show a sample of n rows from the DataFrame.
    """
    df.show(n, truncate=False)

def describe_table(df):
    """
    Print schema and basic statistics of a DataFrame.
    """
    print("Schema:")
    df.printSchema()
    print("\nBasic Statistics:")
    df.describe().show()

def repartition_df(df, num_partitions):
    """
    Repartition a DataFrame to a specified number of partitions.
    """
    return df.repartition(num_partitions)

def cache_df(df):
    """
    Cache a DataFrame in memory.
    """
    return df.cache()

def uncache_df(df):
    """
    Remove a DataFrame from cache.
    """
    df.unpersist()

def get_spark_config():
    """
    Get the current Spark configuration.
    """
    spark = get_spark_session()
    return spark.sparkContext.getConf().getAll()

def print_spark_config():
    """
    Print the current Spark configuration.
    """
    config = get_spark_config()
    for item in config:
        print(f"{item[0]}: {item[1]}")