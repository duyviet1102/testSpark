from pyspark.sql import SparkSession 

#Khoi tao Spark Session
spark = SparkSession.builder.appName("RDDvidu").getOrCreate()

# Tạo RDD từ danh sách
rdd_from_list = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# In noi dung cua RDD
rdd_contents = rdd_from_list.collect()
print("abc xyz", rdd_contents)

even_rdd = rdd_from_list.filter(lambda x: x%2 == 0)
even_rdd.foreach(lambda x: print(x))

# Dong SparkSession
spark.stop()