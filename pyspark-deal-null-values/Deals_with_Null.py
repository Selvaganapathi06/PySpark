import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F, types as T
if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Read Text to RDD - Python")
    sc = SparkContext(conf=conf)
    sqlcontext = SQLContext(sc)
    df = sqlcontext.read \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .load("50_Startups.csv")
    df.show()
    #drop null values
    #Way1:
    df1 = df.na.drop().show()
    #Way2:
    df2 = df.na.drop(subset = ["Administration","MarketingSpend","State","Profit"])
    df2.show()
    