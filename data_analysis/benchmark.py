#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能对比测试：按年份统计平均评分
用于对比 Pandas (单机) vs PySpark (1/2 executors)
"""

import time
import sys

# ==================== 选择执行引擎 ====================
# 通过命令行参数指定引擎: python benchmark.py pandas / spark
engine = sys.argv[1] if len(sys.argv) > 1 else 'spark'

if engine == 'pandas':
    # ---------- Pandas 单机版本 ----------
    import pandas as pd
    print("[Pandas] 开始加载数据...")
    start_load = time.time()
    df = pd.read_csv('/opt/spark/work-dir/douban_movies.csv')
    # 数据清洗与过滤（与Spark清洗逻辑一致）
    # 1. 去除无效行
    df = df.dropna(subset=['movie_id'])
    # 2. 转换类型
    df['rating_score'] = pd.to_numeric(df['rating_score'], errors='coerce')
    df['rating_count'] = pd.to_numeric(df['rating_count'], errors='coerce')
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    # 3. 过滤有效评分（与Spark视角一致）
    df_valid = df[
        (df['rating_score'] > 0) &
        (df['rating_count'] >= 100) &
        (df['year'].notna())
    ].copy()
    print(f"[Pandas] 有效数据量: {len(df_valid)} 条, 加载过滤耗时: {time.time()-start_load:.2f}s")
    
    # 执行 GROUP BY 聚合
    start_query = time.time()
    result = df_valid.groupby('year')['rating_score'].mean().reset_index()
    elapsed = time.time() - start_query
    print(f"[Pandas] 聚合查询耗时: {elapsed:.4f} 秒")
    print("[Pandas] 结果样例（前5年）:")
    print(result.head())
    
elif engine == 'spark':
    # ---------- PySpark 版本（在K8s上运行） ----------
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    
    spark = SparkSession.builder.appName("Benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("[Spark] 开始加载数据...")
    start_load = time.time()
    df_raw = spark.read.option("header", "true").csv("/opt/spark/work-dir/douban_movies.csv")
    # 数据清洗与过滤（与主清洗逻辑一致）
    df_clean = df_raw.filter(col("movie_id").isNotNull())
    df_clean = df_clean.withColumn("year", col("year").cast("int")) \
                       .withColumn("rating_score", col("rating_score").cast("double")) \
                       .withColumn("rating_count", col("rating_count").cast("int"))
    df_valid = df_clean.filter(
        (col("rating_score") > 0) &
        (col("rating_count") >= 100) &
        (col("year").isNotNull())
    )
    df_valid.cache()   # 缓存以便多次查询，但这里只做一次聚合，可以不缓存
    valid_count = df_valid.count()
    print(f"[Spark] 有效数据量: {valid_count} 条, 加载过滤耗时: {time.time()-start_load:.2f}s")
    
    # 执行 GROUP BY 聚合
    start_query = time.time()
    result = df_valid.groupBy("year").avg("rating_score").orderBy("year")
    # 触发 action
    result_list = result.collect()
    elapsed = time.time() - start_query
    print(f"[Spark] 聚合查询耗时: {elapsed:.4f} 秒")
    print("[Spark] 结果样例（前5年）:")
    for row in result_list[:5]:
        print(row)
    
    spark.stop()