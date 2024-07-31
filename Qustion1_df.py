import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg,min,max,count
os.environ[
    "PYSPARK_PYTHON"] = "C:/Users/hp/AppData/Local/Programs/Python/Python37/python.exe"

Spark = SparkSession.builder \
    .appName("Practice") \
    .master("local[*]") \
    .getOrCreate()
schema = "student_id int , name string, score int, subject String"

df = Spark.read\
    .format("csv")\
    .option("header",True)\
    .schema(schema)\
    .option("path","C:/Users/hp/Desktop/prac_df/Student_grade_classification.csv")\
    .load()
    # .load("C:/Users/hp/Desktop/prac_df/Student_grade_classification.csv")

df = df.withColumn("Grade",when(df.score >=90,"A")
                   .when((df.score<90)&(df.score>=80),"B")
                   .when((df.score<80)&(df.score>=70),"C")
                   .when((df.score<=70)&(df.score>=60),"D")
                   .otherwise("E"))

df_avg = df.groupby(df.subject).agg(avg("Score"))
df_max = df.groupby(df.subject).agg(max("Score"))
df_min = df.groupby(df.subject).agg(min("Score"))
df_cnt = df.groupby(df.subject,df.Grade).agg(count("name")).sort(df.subject,df.Grade)

df.show()
df_avg.show()
df_max.show()
df_min.show()
df_cnt.show()
df.printSchema()

