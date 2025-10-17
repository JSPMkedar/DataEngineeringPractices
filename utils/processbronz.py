import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
import logging

def logging_config():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging
logger = logging_config()

# Initialize SparkSession
spark = SparkSession.builder.appName("DataReader").getOrCreate()

def getData(path, isSpark=True):
    if isSpark:
        logger.info("Reading data using Spark...")
        df = spark.read.format("csv").option("header", "true").load(path)
        return df
    else:
        logger.info("Reading data using Pandas...")
        df = pd.read_csv(path)
    return df

def clean_column(df):
    logger.info("Cleaning column..")
    if "_c0" in df.columns:
        df= df.drop("_c0")
        logger.info("Columns cleaned..")
    return df

def writeSilver(df, table):
    try:
        logger.info(f"Writing to silver_{table}..")
        df.write.format("delta").mode("overwrite").saveAsTable(f"workspace.mystore.silver_{table}")
        logger.info(f"Written to silver_{table}..")
        return True
    except Exception as e:
        logger.error(f"Error writing to silver: {e}")
        return False