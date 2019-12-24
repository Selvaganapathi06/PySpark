#Import HeaderFiles
import sys
from pyspark import SparkContext,SparkConf
if __name__ == "__main__":
	#Create Spark Context with spark configuration
	conf = SparkConf().setAppName("wordcount operation")
	sc = SparkContext(conf = conf)
	sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
	sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
	#Create RDD
	rdd = sc.textFile("dhoni.txt")
	print("*****selva*****************")
	#a = rdd.map(lambda x: x.split())
	a = rdd.filter(lambda x: len(x)>0)
	a.coalesce(1).saveAsTextFile("selva")