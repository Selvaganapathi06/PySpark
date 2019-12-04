import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F, types as T
if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Read Text to RDD - Python")
    sc = SparkContext(conf=conf)
    sqlcontext = SQLContext(sc)
    raw_data = sqlcontext.read \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .load("Report1.csv")
    raw_data1 = raw_data
    #Removing Column White Space
    for col in raw_data.columns:
        raw_data1 = raw_data1.withColumnRenamed(col, col.replace(" ", "_"))
    raw_data1.show()
    #Filter
    df1 = raw_data1.filter((raw_data1.OverallScore > 30) & (raw_data1.Overall_Max_Score > 30)).select(raw_data1.OverallScore,raw_data1.Overall_Max_Score)
    df1.show()
    df2 = raw_data1.filter(raw_data1.Result == 'Fail')
    df2.show()
    print("Total Fail:" , df2.count())
    #creating new column
    df3 = raw_data1.withColumn('status', F.lit('don'))
    df3.show()
    df4 = raw_data1.withColumn('updated_OverallScore', raw_data1.OverallScore-50)
    df4.show()
    df5 = df4.select(df4.updated_OverallScore)
    df5.show()
    data_frame_6 = raw_data1.withColumn('FULLNAME',F.concat(raw_data1.First_Name,raw_data1.Last_Name))
    data_frame_7 = data_frame_6.select(data_frame_6.FULLNAME).show()