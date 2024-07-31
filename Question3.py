import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg,min,max,count,sum

os.environ[
    "PYSPARK_PYTHON"] = "C:/Users/hp/AppData/Local/Programs/Python/Python37/python.exe"
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("example")\
    .getOrCreate()

data = [
    (1, "John", 28, 60000),
    (2, "Jane", 32, 75000),
    (3, "Mike", 45, 120000),
    (4, "Alice", 55, 90000),
    (5, "Steve", 62, 110000),
    (6, "Claire", 40, 40000)
]

columns = ["employee_id", "name", "age", "salary"]

df = spark.createDataFrame(data, schema=columns)

df = df.withColumn("age_group ",when(df.age>=50,"Senior")
                   .when((df.age<50)&(df.age>=30),"Mid")
                   .when((df.age<30),"Young").otherwise("NA"))
df = df.withColumn("salary_range  ",when(df.salary >=100000,"High")
                   .when((df.salary <100000)&(df.salary >=50000),"Medium")
                   .when((df.salary <50000),"Low").otherwise("NA"))

df1 = df.filter(col("name").startswith("J"))
df2 = df.filter(col("name").endswith("e"))
df.show()
df1.show()
df2.show()
