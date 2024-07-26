import os
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col

os.environ[
    "PYSPARK_PYTHON"] = "C:/Users/hp/AppData/Local/Programs/Python/Python37/python.exe"  # or "python" depending on your Python executable

sc = SparkContext("local[*]", "spark-program")
# rdd1 = sc.parallelize([("1", "apple"), ("2", "banana"), ("3", "orange")])
# rdd2 = sc.parallelize([("1", "red"), ("2", "yellow"), ("4", "green")])


# rdd1 = sc.parallelize([1, 2, 3, 4, 5])
# rdd2 = sc.parallelize([1,2,3,4,10])
# sub=rdd1.intersection(rdd2).collect()

data = ["apple", "banana", "orange", "pear", "grape"]
rdd=sc.parallelize(data)
searchelement="an"
rdd1=rdd.filter(lambda x:searchelement in x).collect()

for i in rdd1:
    print(i)