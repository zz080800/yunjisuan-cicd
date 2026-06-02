from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("WordCount").getOrCreate()
 
# 读取示例文本（OBS 路径由教师提供）
lines = spark.sparkContext.textFile("s3a://cloud-course-data/sample.txt")
 
word_counts = (
    lines.flatMap(lambda line: line.split())
         .map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a + b)
         .sortBy(lambda x: x[1], ascending=False)
)
 
print("Top 10 words:", word_counts.take(10))
spark.stop()
