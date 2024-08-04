import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg,min,max,count,sum,month

os.environ[
    "PYSPARK_PYTHON"] = "C:/Users/hp/AppData/Local/Programs/Python/Python37/python.exe"
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("quesiton5")\
    .getOrCreate()

Data = [(1,"2023-12-01",1200,"Credit"),
        (2,"2023-11-15",600,"Debit"),
        (3,"2023-12-20",300,"Credit"),
        (4,"2023-10-10",1500,"Debit"),
        (5,"2023-12-30",250,"Credit"),
        (6,"2023-09-25",700,"Debit")]
column = ["transaction_id","transaction_date","amount","transaction_type"]

df = spark.createDataFrame(Data,column)
df = df.withColumn("amount_category",when(df.amount>=1000,"High")
                   .when((df.amount<1000)&(df.amount>500),"Medium")
                   .when(df.amount<=500,"low")
                   .otherwise("NA"))
df = df.withColumn("transact_month",month(df.transaction_date))
df1 = df.filter(col("transact_month").contains(12))
df2 = df.groupby("transaction_type").agg(avg("amount").alias("avg_amount")
                                         ,sum("amount").alias("sum_amount")
                                         ,min("amount").alias("min_amount")
                                         ,max("amount").alias("max_amount"))
df.show()
df1.show()
df2.show()