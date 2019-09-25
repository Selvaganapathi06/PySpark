#import necessary package
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql import Row
if __name__ == "__main__":
	#Create spark conf and context
	conf = SparkConf().setAppName("List to DF")
	sc = SparkContext(conf = conf)
	sqlcontext = SQLContext(sc)
	data = sqlcontext.read\
		   .format("com.databricks.spark.csv")\
		   .option("header", "true")\
           .load("test1.csv")
	data.coalesce(1)\
	.write\
	.format("com.databricks.spark.csv")\
	.option("header", "true")\
	.save("myfile1.csv")