import sys
from pyspark import SparkContext, SparkConf
#conf = SparkConf().setAppName("Read Text to RDD - Python")
conf = SparkConf().setAppName("Read Text to RDD - Python").setMaster("local[2]")
sc = SparkContext(conf=conf)
nums = range(0, 10)
rdd = sc.parallelize(nums,10)
c= rdd.collect()
print("output: ", c)
print("Default parallelism: {}".format(sc.defaultParallelism))
print("Number of partitions: {}".format(rdd.getNumPartitions()))
print("Partitioner: {}".format(rdd.partitioner))
print("Partitions structure: {}".format(rdd.glom().collect()))