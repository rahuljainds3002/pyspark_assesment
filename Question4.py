import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg,min,max,count,sum

os.environ[
    "PYSPARK_PYTHON"] = "C:/Users/hp/AppData/Local/Programs/Python/Python37/python.exe"
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("example")\
    .getOrCreate()

Data =[
    (1 ,"The Matrix", 9, 136),
    (2 ,"Inception",8 ,148),
    (3 ,"The Godfather",9 ,175),
    (4 ,"Toy Story",7 ,81),
    (5 ,"The Shawshank Redemption",10, 142),
    (6 ,"The Silence of the Lambs",8 ,118)
]

columns = ["movie_id","movie_name","rating","duration_minutes"]

df = spark.createDataFrame(Data,schema=columns)

df = df.withColumn("rating_category",when(df.rating >=8,"Excellent")
                   .when((df.rating <8)&(df.rating >=6),"Good")
                   .when((df.rating <6),"Average").otherwise("NA"))

df = df.withColumn("duration_category",when(df.duration_minutes >=150,"Long")
                   .when((df.duration_minutes<150)&(df.duration_minutes>=90),"medium")
                   .when((df.duration_minutes<90),"Short").otherwise("NA"))
df1 = df.filter(col("movie_name").startswith("T"))
df2 = df.filter(col("movie_name").endswith("e"))
df_agg = df.groupBy("rating_category").agg(
    sum(df.duration_minutes).alias("sum_salary"),
    min(df.duration_minutes).alias("min_salary"),
    max(df.duration_minutes).alias("max_salary"),
    avg(df.duration_minutes).alias("average_salary")
)
df.show()
df1.show()
df2.show()
df_agg.show()


