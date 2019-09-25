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
	list12 = [["selva","salem","TN"],["ganapathi","thirumanur","TN"],["sivanesh","agastheswaram","TN"]]
	schema = StructType([StructField("Name", StringType(),True),StructField("Place", StringType(),True),StructField("State", StringType(),True)])
	data =sqlcontext.createDataFrame(list12,schema)
	data.coalesce(1)\
	.write\
	.format("com.databricks.spark.csv")\
	.option("header", "true")\
	.save("myfile.csv")