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
           .load("pokemon_data.csv")
	#data.show() normally show top 20 values
	#if you want show one or two value means directly specify n =2	
	data.show(n=10)
	#To Check all dataframe columns
	##https://github.com/tirthajyoti/Spark-with-Python/blob/master/Python-and-Spark-for-Big-Data-master/Spark_DataFrame_Project_Exercise/Spark%20DataFrames%20Project%20Exercise%20-%20SOLUTIONS.ipynb
	print(data.columns)
	#To seeing schema 
	print("***************")
	data.printSchema()
	print("********Describe********")
	c = data.describe()
	print(c)
	print("********Describe111********")
	data.describe().show()
	#Added new column in data frame
	data1=data.withColumn("selva",data['Attack']/data['Hp'])
	print("**selva**")
	data1.select('selva').show()
	#Temp table creation
	data.createOrReplaceTempView("selva123")
	print("********Temptable creation***********")
	data3 =sqlcontext.sql("select * from selva123")
	data3.show()
	#select where condition in sql
	data4=sqlcontext.sql("select * from selva123 where Attack >40")
	data4.show()
	data5=sqlcontext.sql("select Attack from selva123 where Attack >80").show()