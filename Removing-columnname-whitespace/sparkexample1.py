import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F, types as T
if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Read Text to RDD - Python")
    sc = SparkContext(conf=conf)
    sqlcontext = SQLContext(sc)
    raw_data = sqlcontext.read \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .load("Report1.csv")

    #Removing Column White Space
    raw_data1 = raw_data
    print(raw_data1)
    for col in raw_data.columns:
        raw_data1 = raw_data1.withColumnRenamed(col, col.replace(" ", "_"))
    #raw_data1.show()

