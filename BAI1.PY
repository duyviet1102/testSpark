import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col,split, explode, count, lower
spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header','true').load('ex1.csv')
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, row_number
from pyspark.sql.window import Window
#Câu1
# print("<-----  Chào Long Nguuuu!!!!  ----->")
# print("Câu 1:")
# total_transactions = data.select("InvoiceNo").distinct().count()
# print("Tổng số giao dịch là:", total_transactions)
# total_products = data.select("StockCode").distinct().count()
# print("Tổng số sản phẩm là:", total_products)
# total_customers = data.select("CustomerID").distinct().count()
# print("Tổng số khách hàng là:", total_customers)
#cau2

# khachhang = data.count()
# khachhang_khongcodulieu = data.filter(data["CustomerID"].isNull()).count()
# missing_info_percentage = (khachhang_khongcodulieu/ khachhang) * 100 
# print( f"ti le khach hang khong co thong tin : {missing_info_percentage : .2f}")
# spark.stop() 

#cau3 
spark = SparkSession.builder.appName("test").getOrCreate()
# Tạo cột mới "TotalQuantity" bằng cách tính tổng Quantity cho mỗi quốc gia
data = data.groupBy("Country").agg({"Quantity": "sum"}).withColumnRenamed("sum(Quantity)", "TotalQuantity")

# Sắp xếp dữ liệu giảm dần theo TotalQuantity
data = data.orderBy(desc("TotalQuantity"))

# Sử dụng window function để gán số thứ tự cho mỗi quốc gia
windowSpec = Window.orderBy(desc("TotalQuantity"))
data = data.withColumn("row_num", row_number().over(windowSpec))

# Lấy quốc gia có số lượng đơn hàng nhiều thứ 3
third_most_orders_country = data.filter(data.row_num == 3).select("Country", "TotalQuantity")

# In ra kết quả
third_most_orders_country.show()
#cau4  
#Cau4
spark = SparkSession.builder.appName("Tu xuat hien it nhat").getOrCreate()
print("Cau 4:")
file_path = 'ex1.csv'
data = spark.read.csv(file_path, header=True, inferSchema=True)
words = data.select(explode(split(lower(col("Description")), r'\W+')).alias("word"))
word_counts = words.groupBy("word").agg(count("word").alias("count"))
min_count = word_counts.agg({"count":"min"}).collect()[0][0]
least_occured_words = word_counts.filter(col("count") == min_count).select("word", "count")
print(f"Tu xuat hien it nhat trong Description:")
least_occured_words.show()
print(least_occured_words.count())
spark.stop()

#Cau5
spark = SparkSession.builder.appName("San pham ban chay nhat tai UK").getOrCreate()
print("Cau 5:")
file_path = 'ex1.csv'
data = spark.read.csv(file_path, header=True, inferSchema=True)
uk_data = data.filter(col("Country") == "United Kingdom")
product_sales = uk_data.groupBy("StockCode").agg(sum("Quantity").alias("TotalQuantity")).sort(col("TotalQuantity").desc())
best_selling_product = product_sales.first()
print(f"San pham ban chay nhat tai UK la: {best_selling_product['StockCode']}, voi so luong: {best_selling_product['TotalQuantity']}")
spark.stop() 
#toi hat 