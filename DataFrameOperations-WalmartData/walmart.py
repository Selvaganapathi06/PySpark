#import necessary package
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.functions import mean
from pyspark.sql import functions as F
from pyspark.sql import Row
if __name__ == "__main__":
	#Create spark conf and context
	conf = SparkConf().setAppName("List to DF")
	sc = SparkContext(conf = conf)
	sqlcontext = SQLContext(sc)
	data = sqlcontext.read\
		   .format("com.databricks.spark.csv")\
		   .option("header", "true")\
           .load("walmart.csv")
	data.show() 
	#normally show top 20 values
	#if you want show one or two value means directly specify n =2	
	#data.show()
		
	df1 = data.select("Open","High","Low","Close")
	df1.show()
	#Aggregation operations
	#Need to import sql functions
	df2 = data.select(mean("Open"))
	df2.show()
	df3 = data.select(F.max("Low"))
	df3.show()
	df4 = data.select(F.min("High"))
	df4.show()
	df5 = data.select(F.sum("Close"))	
	df5.show()
	#To round() to round the decimal value
	df5 = data.select(F.round(F.avg("Close")))	
	df5.show()
	data.printSchema()
	#What day had the Peak High in Price?
	df6=data.orderBy(data["High"].desc())
	df6.show(n=5)
	#TO find out mean of close column
	df7=data.select(F.round(mean("Close")))
	df7.show()
	#To find the max and min value in volume column
	df8 = data.select(F.max("Volume"),F.min("Volume"))
	df8.show()
	#How many days was the Close lower than 60 dollars?
	#Way1
	df9 = data.filter("Close<60").count()
	print("***********************Selva******************************")
	print(df9)
	#Way2
	res=data.filter(data["Close"]<60)
	df10 = res.select(F.count("Close"))
	df10.show()
	##What is the Pearson correlation between High and Volume?
	df11 = data.select(F.corr("High","Volume"))
	df11.show()
	
	
	