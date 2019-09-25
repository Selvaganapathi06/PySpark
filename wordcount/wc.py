#Import HeaderFiles
import sys
from pyspark import SparkContext,SparkConf
if __name__ == "__main__":
	#Create Spark Context with spark configuration
	conf = SparkConf().setAppName("wordcount operation")
	sc = SparkContext(conf = conf)
	#Create RDD
	CRDD = sc.textFile("C:\spark\sample\wordcount\wordcount.txt")
	print("*****selva*****************")
	print(CRDD.collect())
	print("*************")
	#print(CRDD.count())
	#dd = CRDD.collect()
	#print(dd)
	