#import headers
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql import Row
if __name__ == "__main__":
	#Create Spark context with spark configuration
	conf = SparkConf().setAppName("CSV USED UNION OPERATION")
	sc = SparkContext(conf=conf)
	sqlcon = SQLContext(sc)
	df = sqlcon.read.format("com.databricks.spark.csv")\
		.option("header", "true")\
		.load("Uni.csv")
	df.createOrReplaceTempView("df")
	df.show()
	df1 =sqlcon.sql("select A,B,D from df")
	df1.createOrReplaceTempView("df1")
	df1.show()
	df2 =sqlcon.sql("select A,c,D from df")
	df2.createOrReplaceTempView("df2")
	df2.show()
	df3 = df1.union(df2)
	df3.createOrReplaceTempView("df3")
	df3.show()
	sc.stop()