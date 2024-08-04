import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg,min,max,count,sum,month,to_date

os.environ[
    "PYSPARK_PYTHON"] = "C:/Users/hp/AppData/Local/Programs/Python/Python37/python.exe"
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("Question5")\
    .getOrCreate()
sch = "transaction_id int ,customer_id int ,transaction_amount int ,transaction_date string"
df = spark.read\
    .format("csv")\
    .schema(sch)\
    .option("header",True)\
    .option("path","C:/Users/hp/Desktop/prac_df/Cust_tran_analysis.csv") \
    .load()
df = df.withColumn("date_new",to_date(df.transaction_date,"dd-MM-yyyy"))
df.show()