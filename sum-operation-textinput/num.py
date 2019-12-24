#Import HeaderFiles
import sys
from pyspark import SparkContext,SparkConf
if __name__ == "__main__":
	#Create Spark Context with spark configuration
	conf = SparkConf().setAppName("wordcount operation")
	sc = SparkContext(conf = conf)
	#Create RDD
	rdd11 = sc.textFile("nimber1.txt")
	print(rdd11.collect())
	numbers = rdd11.flatMap(lambda line: line.split("\t"))
	print(numbers.collect())	
	validNumbers = numbers.filter(lambda number: number)
	
	intNumbers = validNumbers.map(lambda number: int(number))
	print(intNumbers.collect())
	print("Sum is: {}".format(intNumbers.reduce(lambda x, y: x + y)))
	