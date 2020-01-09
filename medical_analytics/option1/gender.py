from pyspark.sql import functions as F
# File location and type
file_location = "/FileStore/tables/medi.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dataFrame = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)


#Select Required Column
sltCol = dataFrame.select("Gender","treatment")

#Treatment Taken People
caseStmt = sltCol.select("Gender",(F.when(F.col("treatment")=="Yes", 1).otherwise(0)).alias("Yes"),(F.when(F.col("treatment")=="No", 1).otherwise(0)).alias("No"))

#Creating Temporary View
caseStmt.createOrReplaceTempView("TempViewTable")

#Way1 for 
a = spark.sql("select Yes,No, case when Gender in('M','Male') then 'Male' when Gender in('Female') then 'Female' END as Gender from TempViewTable")

#way2


#Applying regular expression and replace gender values
#newDf = caseStmt.withColumn('Gender', F.regexp_replace('Gender', 'M', 'Male'))
#newDf1 = newDf.withColumn('Gender', F.regexp_replace('Gender', 'Maleale', 'Male'))

newDf2 = a.groupBy("Gender").agg(F.sum("Yes"),F.sum("No"))

display(newDf2)