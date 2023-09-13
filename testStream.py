# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext

# # Khởi tạo SparkContext
# sc = SparkContext("local", "SparkDStreamDemo")

# # Khởi tạo StreamingContext với batch interval là 1 giây
# ssc = StreamingContext(sc, 1)

# # Đọc dữ liệu từ file văn bản "ex1.csv"
# file_path = "test.txt"
# lines = ssc.textFileStream(file_path)

# # Xử lý dữ liệu trong DStream (ví dụ: in ra màn hình)
# lines.pprint()

# # Khởi động quá trình streaming
# ssc.start()

# # Chờ quá trình streaming kết thúc
# ssc.awaitTermination()

# from pyspark import SparkContext 
# from pyspark.streaming import StreamingContext
# sc = SparkContext("local", "testSpark")
# ssc = StreamingContext(sc,1)
# lines = ssc.socketTextStream("localhost",2002)
# words = lines.flatMap(lambda line: line.split(""))
# pairs = words.map(lambda word:(word,1))
# wordCounts = pairs.reduceByKey(lambda x,y: x+y)
# wordCounts.pprint()
# ssc.start()
# ssc.awaitTermination( 
# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# sc = SparkContext("local[2]", "NetworkWordCount")
# ssc = StreamingContext(sc, 1)
# # Create a DStream that will connect to hostname:port, like localhost:9999
# lines = ssc.socketTextStream("localhost", 2222)
# # Split each line into words
# words = lines.flatMap(lambda line: line.split(" "))
# # Count each word in each batch
# pairs = words.map(lambda word: (word, 1))
# wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# # Print the first ten elements of each RDD generated in this DStream to the console
# wordCounts.pprint()
# ssc.start()             # Start the computation
# ssc.awaitTermination()  # Wait for the computation to terminate
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Tạo SparkContext
sc = SparkContext("local[2]", "testSpark")

# Tạo StreamingContext với batch interval là 1 giây
ssc = StreamingContext(sc, 3)

# Tạo DStream từ nguồn dữ liệu socket, lắng nghe trên cổng 9999
lines = ssc.socketTextStream("localhost", 2222)
# Xử lý dữ liệu ở đây
numbers = lines.flatMap(lambda line: line.split(" ")).map(lambda x: int(x))
sums = numbers.reduce(lambda x, y: x + y)
lines.pprint()

# Khởi động Spark Streaming
ssc.start()

# Chờ đợi cho đến khi ứng dụng dừng (hoặc có thể dừng bằng cách nhấn Ctrl+C)
ssc.awaitTermination()