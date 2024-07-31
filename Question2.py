import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg,min,max,count,sum

os.environ[
    "PYSPARK_PYTHON"] = "C:/Users/hp/AppData/Local/Programs/Python/Python37/python.exe"

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("quesiton2")\
    .getOrCreate()
schema = "product_id int,product_name String,price int,category String "
df = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("path","C:/Users/hp/Desktop/prac_df/Ecom_Product_analysis.csv")\
    .load()
df = df.withColumn("price_category",when(df.price>500,"Expensive")
              .when((df.price<500)&(df.price>=200),"Moderate")
              .otherwise("Cheap"))
# df.col("product_name").startswith("s").show()
df.filter(col("product_name").startswith("S")).show()
df.filter(col("product_name").endswith("s")).show()
df_new = df.groupby("category").agg(sum(df.price).alias("sum_price"),
                                    min(df.price).alias("min_price"),
                                    max(df.price).alias("max_price"),
                                    avg(df.price).alias("avg_price"))
df.show()
df_new.show()