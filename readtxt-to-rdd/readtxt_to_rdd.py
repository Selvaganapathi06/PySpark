import sys
#import org.apache.log4j.{Level, Logger}
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":
 
  # create Spark context with Spark configuration
	conf = SparkConf().setAppName("Read Text to RDD - Python")
	sc = SparkContext(conf=conf)
 
  # read input text file to RDD
	lines = sc.textFile("C:\spark\sample\sam.txt,C:\spark\sample\sam1.txt,C:\spark\sample\sam2.txt")
  #print(lines)
	lines.coalesce(1).saveAsTextFile("C:\spark\sample\selva")
  
	
  # collect the RDD to a list
	llist = lines.collect()
  #print(llist)
  #llist.saveAsTextFile("C:\spark\sample\writ.txt")
 
  # print the list
	for line in llist:
		print(line)