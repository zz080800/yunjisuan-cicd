#!/usr/bin/env python3
import sys
import os

# 强制无缓冲输出
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 1)

print("="*60, flush=True)
print("DEBUG: Script started", flush=True)
print("Python executable:", sys.executable, flush=True)
print("Python version:", sys.version, flush=True)
print("Current directory:", os.getcwd(), flush=True)
print("Files in work dir:", os.listdir('/opt/spark/work/') if os.path.exists('/opt/spark/work/') else 'not found', flush=True)
print("="*60, flush=True)

print("Step 1: Importing pyspark...", flush=True)
from pyspark.sql import SparkSession
print("Step 1: OK", flush=True)

print("Step 2: Creating SparkSession...", flush=True)
spark = SparkSession.builder \
    .appName("Debug") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
print("Step 2: OK", flush=True)
print("Spark version:", spark.version, flush=True)

print("Step 3: Creating test RDD...", flush=True)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
result = rdd.sum()
print("Step 3: OK - Sum =", result, flush=True)

print("Step 4: Testing OBS access...", flush=True)
try:
    # 只列出桶中的文件，不读取整个CSV
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI("s3a://yunjisuan-data1/"), hadoop_conf)
    status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path("s3a://yunjisuan-data1/"))
    files = [str(s.getPath().getName()) for s in status]
    print("Step 4: OK - Files in bucket:", files[:10], flush=True)
except Exception as e:
    print(f"Step 4: ERROR - {type(e).__name__}: {e}", flush=True)

print("Step 5: Stopping Spark...", flush=True)
spark.stop()
print("DEBUG: Script finished successfully!", flush=True)