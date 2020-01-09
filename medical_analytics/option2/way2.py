from pyspark.sql import functions as F
from pyspark.sql.functions import udf


# File location and type
file_location = "/FileStore/tables/medi.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

def parse_gender(gen):
  if gen.lower() in('m,male,mal,ma'):
    return "Male"
  elif  gen.lower() in('female,fem'):
    return "Female"
  else:
    return "Transgender"
 
parse_gender_udf = udf(parse_gender)

#The applied options are for CSV files. For other file types, these will be ignored.
dataFrame = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)


#Select Required Column
sltCol = dataFrame.select("Gender","treatment")

#Treatment Taken People
caseStmt = sltCol.select("Gender",(F.when(F.col("treatment")=="Yes", 1).otherwise(0)).alias("Yes"),(F.when(F.col("treatment")=="No", 1).otherwise(0)).alias("No"))


colGender = caseStmt.select((parse_gender_udf("Gender")).alias("Gender"),"Yes","No")


newDf2 = colGender.groupBy("Gender").agg(F.sum("Yes"),F.sum("No"))

display(newDf2)