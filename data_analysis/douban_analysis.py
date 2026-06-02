#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
豆瓣电影数据集清洗 + 特征工程
专门处理 CSV 格式问题
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, size, split, udf, regexp_replace, explode
from pyspark.sql.types import IntegerType, DoubleType, ArrayType, StringType

# ==================== 初始化 Spark ====================
spark = SparkSession.builder \
    .appName("DoubanDataCleaning") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("开始加载豆瓣电影数据集...")
print("=" * 60)

# ==================== 1. 读取原始数据（使用更宽松的模式） ====================
INPUT_PATH = "file:///opt/spark/work-dir/douban_movies.csv"

# 使用更宽松的读取选项
df_raw = spark.read \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv(INPUT_PATH)

print(f"\n原始读取行数: {df_raw.count()}")

# 过滤掉损坏的记录和空行
if "_corrupt_record" in df_raw.columns:
    df_raw = df_raw.filter(col("_corrupt_record").isNull())
    df_raw = df_raw.drop("_corrupt_record")

# 删除所有字段都为 null 的行
df_raw = df_raw.dropna(how="all")

# 只保留有 movie_id 的行（主键非空）
df_raw = df_raw.filter(col("movie_id").isNotNull())

print(f"清洗无效行后行数: {df_raw.count()}")

# 手动转换数据类型（先处理空值）
df_raw = df_raw \
    .withColumn("year_temp", regexp_replace(col("year"), "[^0-9]", "")) \
    .withColumn("year", 
        when(col("year_temp").cast(IntegerType()).isNotNull(), col("year_temp").cast(IntegerType()))
        .otherwise(lit(None))) \
    .drop("year_temp")

df_raw = df_raw \
    .withColumn("rating_score_temp", regexp_replace(col("rating_score"), "[^0-9.]", "")) \
    .withColumn("rating_score", 
        when(col("rating_score_temp").cast(DoubleType()).isNotNull(), col("rating_score_temp").cast(DoubleType()))
        .otherwise(lit(None))) \
    .drop("rating_score_temp")

df_raw = df_raw \
    .withColumn("rating_count_temp", regexp_replace(col("rating_count"), "[^0-9]", "")) \
    .withColumn("rating_count", 
        when(col("rating_count_temp").cast(IntegerType()).isNotNull(), col("rating_count_temp").cast(IntegerType()))
        .otherwise(lit(None))) \
    .drop("rating_count_temp")

df_raw = df_raw \
    .withColumn("collect_count_temp", regexp_replace(col("collect_count"), "[^0-9]", "")) \
    .withColumn("collect_count", 
        when(col("collect_count_temp").cast(IntegerType()).isNotNull(), col("collect_count_temp").cast(IntegerType()))
        .otherwise(lit(None))) \
    .drop("collect_count_temp")

print("\n[1] 原始数据加载完成")
print(f"原始行数: {df_raw.count()}")
print("\nSchema 信息:")
df_raw.printSchema()
print("\n前 5 行预览:")
df_raw.select("movie_id", "title", "year", "rating_score", "rating_count", "genres", "countries").show(5, truncate=30)

# ==================== 2. 缺失值统计 ====================
print("\n[2] 缺失值统计 (各字段缺失比例):")
total_rows = df_raw.count()
if total_rows > 0:
    for col_name in df_raw.columns:
        null_count = df_raw.filter(col(col_name).isNull()).count()
        ratio = null_count / total_rows
        if ratio > 0:
            print(f"  {col_name}: {null_count} ({ratio:.2%})")

# ==================== 3. 缺失值处理 ====================
print("\n[3] 开始处理缺失值...")

# 创建副本
df_clean = df_raw

# 填充分类字段
for field in ["genres", "countries", "directors"]:
    if field in df_clean.columns:
        df_clean = df_clean.fillna({field: "Unknown"})

# 填充 summary
if "summary" in df_clean.columns:
    df_clean = df_clean.fillna({"summary": ""})

# 年份填充中位数（只对有值的行计算）
if df_clean.filter(col("year").isNotNull()).count() > 0:
    median_year = df_clean.filter(col("year").isNotNull()).approxQuantile("year", [0.5], 0.01)[0]
    df_clean = df_clean.fillna({"year": median_year})
    print(f"  年份中位数: {median_year}")

# 删除评分和评分人数都为空的记录
df_clean = df_clean.dropna(subset=["rating_score", "rating_count"], how="all")

# ==================== 4. 异常值处理 ====================
print("\n[4] 处理异常值...")

before_count = df_clean.count()

# 年份异常：保留有效年份（1900-2026）或空值
df_clean = df_clean.filter(
    ((col("year") >= 1900) & (col("year") <= 2026)) | col("year").isNull()
)

# 评分异常：保留有效评分（0-10）或空值
df_clean = df_clean.filter(
    ((col("rating_score") >= 0) & (col("rating_score") <= 10)) | col("rating_score").isNull()
)

# 评分人数异常：保留非负数或空值
df_clean = df_clean.filter(
    (col("rating_count") >= 0) | col("rating_count").isNull()
)

removed_count = before_count - df_clean.count()
print(f"  异常值过滤后: 移除了 {removed_count} 条记录")
print(f"  当前记录数: {df_clean.count()}")

# ==================== 5. 去除重复记录 ====================
print("\n[5] 去除重复记录...")
duplicate_before = df_clean.count()
df_clean = df_clean.dropDuplicates(["movie_id"])
duplicate_after = df_clean.count()
print(f"  去重前: {duplicate_before}, 去重后: {duplicate_after}, 移除: {duplicate_before - duplicate_after}")

# ==================== 6. 构造新特征 ====================
print("\n[6] 构造新特征...")

# 将 genres 拆分成数组
def split_genres(s):
    if s and s != "Unknown":
        # 同时支持逗号和斜杠分隔
        # 先把斜杠替换为逗号，再按逗号拆分
        s = s.replace("/", ",")
        return [g.strip() for g in s.split(",") if g.strip()]
    return []
split_genres_udf = udf(split_genres, ArrayType(StringType()))
df_clean = df_clean.withColumn("genres_array", split_genres_udf(col("genres")))
df_clean = df_clean.withColumn("genres_count", size(col("genres_array")))

# 国家数量
def count_countries(s):
    if s and s != "Unknown":
        return len(s.split(","))
    return 0

count_countries_udf = udf(count_countries, IntegerType())
df_clean = df_clean.withColumn("countries_count", count_countries_udf(col("countries")))

# 评分等级
df_clean = df_clean.withColumn("rating_level",
    when(col("rating_score") >= 8.0, "high")
    .when(col("rating_score") >= 6.0, "medium")
    .when(col("rating_score").isNull(), "unknown")
    .otherwise("low")
)

# 年代区间
df_clean = df_clean.withColumn("decade", 
    when(col("year").isNotNull(), (col("year") / 10).cast(IntegerType()) * 10)
    .otherwise(lit(0))
)

# ==================== 7. 输出清洗结果 ====================
print("\n[7] 清洗前后行数对比:")
print(f"清洗前: {df_raw.count()} 行")
print(f"清洗后: {df_clean.count()} 行")
print(f"移除记录数: {df_raw.count() - df_clean.count()}")

# ==================== 8. 统计信息 ====================
print("\n[8] 数值字段基本统计信息:")
numeric_cols = ["year", "rating_score", "rating_count"]
available_cols = [c for c in numeric_cols if c in df_clean.columns]
if available_cols:
    df_clean.select(available_cols).describe().show()

# ==================== 9. 注册临时视图 ====================
df_clean.createOrReplaceTempView("movies")
print("\n临时视图 'movies' 已注册")

# 展示结果
print("\n[示例] 按年代区间统计平均评分:")
df_clean.filter(col("decade") > 0).groupBy("decade").avg("rating_score").orderBy("decade").show(20)

print("\n[示例] 按评分等级统计:")
df_clean.groupBy("rating_level").count().orderBy("rating_level").show()

print("\n[示例] 各类型电影数量 TOP 10:")
df_clean.select(explode("genres_array").alias("genre")).filter(col("genre") != "").groupBy("genre").count().orderBy(col("count").desc()).show(10)

print("\n数据清洗任务完成！")

# ==================== A-2 统计分析 ====================
# 以下代码追加在 spark.stop() 之前，利用已注册的 movies 临时视图

print("\n" + "="*80)
print("开始 A-2 Spark SQL 统计分析（有效评分视角）")
print("="*80)

# ------------------------------------------------------------
# 0. 创建有效评分视图（过滤无效数据）
# ------------------------------------------------------------
print("\n[0] 创建有效评分视图（过滤评分=0、评分为空、评分人数<100的记录）")
print("说明：豆瓣数据中存在大量评分为0的记录，这些通常是未评分的电影，")
print("      为获得更有意义的分析结果，后续分析将排除这些无效数据。")

valid_movies = spark.sql("""
    SELECT *
    FROM movies
    WHERE rating_score IS NOT NULL 
      AND rating_score > 0
      AND rating_count IS NOT NULL
      AND rating_count >= 100
""")
valid_movies.createOrReplaceTempView("valid_movies")

# 统计过滤情况
valid_count = spark.sql("SELECT COUNT(*) AS cnt FROM valid_movies").collect()[0][0]
total_count = spark.sql("SELECT COUNT(*) AS cnt FROM movies WHERE rating_score IS NOT NULL").collect()[0][0]
print(f"有效电影数量: {valid_count}")
print(f"原始有评分电影数量: {total_count}")
print(f"过滤比例: {(total_count - valid_count) * 100.0 / total_count:.1f}% (评分=0或评分人数<100)")

# ------------------------------------------------------------
# 1. 评分分布与评分趋势分析
# ------------------------------------------------------------

# 1.1 评分等级分布（GROUP BY 聚合）
print("\n[1.1] 评分等级分布（按 rating_level 分组）- 有效评分视角")
rating_dist = spark.sql("""
    SELECT 
        rating_level,
        COUNT(*) AS movie_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM valid_movies), 2) AS percentage
    FROM valid_movies
    GROUP BY rating_level
    ORDER BY 
        CASE rating_level 
            WHEN 'high' THEN 1 
            WHEN 'medium' THEN 2 
            WHEN 'low' THEN 3 
            ELSE 4 
        END
""")
rating_dist.show(truncate=False)

# 1.2 高分电影特征分析：评分 ≥ 8.5 的电影
print("\n[1.2] 高分电影特征分析（评分 ≥ 8.5）- 有效评分视角")
high_rate_movies = spark.sql("""
    SELECT 
        movie_id, title, year, rating_score, rating_count,
        genres, countries, decade, genres_array
    FROM valid_movies
    WHERE rating_score >= 8.5
    ORDER BY rating_score DESC, rating_count DESC
    LIMIT 100
""")
high_rate_movies.createOrReplaceTempView("high_rate_movies")

# 高分电影的类型分布
print("\n高分电影（≥8.5）的类型分布 TOP 10:")
genre_high = spark.sql("""
    SELECT 
        genre,
        COUNT(*) AS movie_count
    FROM (
        SELECT explode(genres_array) AS genre
        FROM high_rate_movies
        WHERE genres_array IS NOT NULL
    )
    GROUP BY genre
    ORDER BY movie_count DESC
    LIMIT 10
""")
genre_high.show(truncate=False)

# 高分电影的国家分布
print("\n高分电影（≥8.5）的国家分布 TOP 10:")
country_high = spark.sql("""
    SELECT 
        TRIM(country) AS country,
        COUNT(*) AS movie_count
    FROM (
        SELECT explode(split(countries, ',')) AS country
        FROM high_rate_movies
        WHERE countries IS NOT NULL AND countries != 'Unknown'
    )
    GROUP BY country
    ORDER BY movie_count DESC
    LIMIT 10
""")
country_high.show(truncate=False)

# 高分电影的年代分布
print("\n高分电影（≥8.5）的年代分布（按 decade）:")
decade_high = spark.sql("""
    SELECT 
        decade,
        COUNT(*) AS movie_count
    FROM high_rate_movies
    WHERE decade > 0
    GROUP BY decade
    ORDER BY decade
""")
decade_high.show(truncate=False)

# 1.3 热度与评分的关系
print("\n[1.3] 热度与评分关系（评分人数 vs 评分）- 有效评分视角")
heat_relation = spark.sql("""
    SELECT rating_score, rating_count
    FROM valid_movies
    ORDER BY rating_count DESC
    LIMIT 50
""")
heat_relation.show(10, truncate=False)

# ------------------------------------------------------------
# 2. 年份与时间趋势分析
# ------------------------------------------------------------

# 2.1 电影产量与评分随年份变化（时间维度趋势分析）
print("\n[2.1] 电影产量与平均评分逐年变化（2000-2020）- 有效评分视角")
yearly_stats = spark.sql("""
    SELECT 
        year,
        COUNT(*) AS total_movies,
        ROUND(AVG(rating_score), 2) AS avg_rating,
        ROUND(SUM(rating_count), 0) AS total_ratings
    FROM valid_movies
    WHERE year BETWEEN 2000 AND 2020
    GROUP BY year
    ORDER BY year
""")
yearly_stats.show(25, truncate=False)

# 2.2 “叫好不叫座” vs “叫座不叫好”（使用窗口函数）
print("\n[2.2] 叫好不叫座 vs 叫座不叫好分析（近10年，2014-2023）- 有效评分视角")
call_analysis = spark.sql("""
    WITH yearly_median AS (
        SELECT 
            year,
            PERCENTILE_APPROX(rating_count, 0.5) AS median_rating_count
        FROM valid_movies
        WHERE year BETWEEN 2014 AND 2023
        GROUP BY year
    ),
    movie_with_median AS (
        SELECT 
            m.year, m.title, m.rating_score, m.rating_count,
            ym.median_rating_count
        FROM valid_movies m
        JOIN yearly_median ym ON m.year = ym.year
    )
    SELECT 
        '叫好不叫座' AS category,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating,
        ROUND(AVG(rating_count), 0) AS avg_rating_count
    FROM movie_with_median
    WHERE rating_score >= 8.5 AND rating_count < median_rating_count
    UNION ALL
    SELECT 
        '叫座不叫好' AS category,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating,
        ROUND(AVG(rating_count), 0) AS avg_rating_count
    FROM movie_with_median
    WHERE rating_score < 6.0 AND rating_count > median_rating_count
""")
call_analysis.show(truncate=False)

# ------------------------------------------------------------
# 3. 类型分析与题材偏好变化
# ------------------------------------------------------------

# 3.1 类型分布与评分高低
print("\n[3.1] 各类型的电影数量及平均评分（按数量降序）- 有效评分视角")
type_stats = spark.sql("""
    SELECT 
        genre,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating,
        ROUND(PERCENTILE_APPROX(rating_score, 0.5), 2) AS median_rating
    FROM (
        SELECT explode(genres_array) AS genre, rating_score
        FROM valid_movies
        WHERE genres_array IS NOT NULL
    )
    GROUP BY genre
    ORDER BY movie_count DESC
""")
type_stats.show(15, truncate=False)

# 3.2 跨年代的类型偏好变迁
print("\n[3.2] 跨年代的类型偏好变迁（按 decade 分组，每 decade 取 Top 3 类型）- 有效评分视角")
decade_top_types = spark.sql("""
    WITH decade_types AS (
        SELECT 
            decade,
            genre,
            COUNT(*) AS movie_count,
            ROW_NUMBER() OVER (PARTITION BY decade ORDER BY COUNT(*) DESC) AS rank
        FROM (
            SELECT decade, explode(genres_array) AS genre
            FROM valid_movies
            WHERE decade > 0 AND genres_array IS NOT NULL
        )
        GROUP BY decade, genre
    )
    SELECT decade, genre, movie_count
    FROM decade_types
    WHERE rank <= 3
    ORDER BY decade, rank
""")
decade_top_types.show(50, truncate=False)

# 3.3 类型组合模式分析
print("\n[3.3] 高频类型组合分析（两个类型组合，出现在同一部电影中）- 有效评分视角")
type_pairs = spark.sql("""
    WITH exploded AS (
        SELECT movie_id, genre
        FROM valid_movies
        LATERAL VIEW explode(genres_array) t AS genre
        WHERE SIZE(genres_array) >= 2 AND genre != ''
    ),
    pairs AS (
        SELECT a.movie_id, a.genre AS genre1, b.genre AS genre2
        FROM exploded a
        JOIN exploded b ON a.movie_id = b.movie_id
        WHERE a.genre < b.genre
    )
    SELECT 
        CONCAT(genre1, ' + ', genre2) AS genre_pair,
        COUNT(DISTINCT movie_id) AS pair_count
    FROM pairs
    GROUP BY genre1, genre2
    ORDER BY pair_count DESC
    LIMIT 20
""")
type_pairs.show(20, truncate=False)

# ------------------------------------------------------------
# 4. 国家与地区电影产业分析
# ------------------------------------------------------------

# 4.1 各国家电影产量与评分对比
print("\n[4.1] 各国家电影产量与平均评分（过滤总数 >= 10）- 有效评分视角")
country_stats = spark.sql("""
    WITH country_expanded AS (
        SELECT 
            TRIM(country) AS country,
            rating_score
        FROM valid_movies
        LATERAL VIEW explode(split(countries, ',')) t AS country
        WHERE countries IS NOT NULL AND countries != 'Unknown'
    )
    SELECT 
        country,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating
    FROM country_expanded
    GROUP BY country
    HAVING COUNT(*) >= 10
    ORDER BY movie_count DESC
""")
country_stats.show(20, truncate=False)

# 4.2 中外合拍片分析
print("\n[4.2] 中外合拍片 vs 纯国产片 vs 纯引进片评分对比 - 有效评分视角")
co_production = spark.sql("""
    SELECT 
        CASE 
            WHEN countries LIKE '%中国%' AND countries LIKE '%,%' THEN '中外合拍'
            WHEN countries LIKE '%中国%' AND countries NOT LIKE '%,%' THEN '纯国产'
            WHEN countries NOT LIKE '%中国%' AND countries NOT LIKE '%,%' THEN '纯引进'
            ELSE '其他'
        END AS film_type,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating
    FROM valid_movies
    WHERE countries IS NOT NULL AND countries != 'Unknown'
    GROUP BY film_type
    HAVING film_type IN ('中外合拍', '纯国产', '纯引进')
""")
co_production.show(truncate=False)

# ------------------------------------------------------------
# 5. 导演与演员影响力分析
# ------------------------------------------------------------

# 添加 directors_array 列
from pyspark.sql.functions import split, when, trim, explode as explode_f
valid_movies_df = spark.table("valid_movies")
movies_with_dir = valid_movies_df.withColumn(
    "directors_clean",
    when(col("directors").isNull(), lit(""))
     .otherwise(regexp_replace(col("directors"), ",", "/"))
).withColumn(
    "directors_array",
    split(col("directors_clean"), "/")
)
movies_with_dir.createOrReplaceTempView("valid_movies_with_dir")

# 5.1 高产/高分导演排行
print("\n[5.1] 高产导演 TOP 20（作品数量最多）- 有效评分视角")
high_prod_directors = spark.sql("""
    SELECT 
        TRIM(director) AS director,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating
    FROM (
        SELECT explode(directors_array) AS director, rating_score
        FROM valid_movies_with_dir
        WHERE directors_array IS NOT NULL
    )
    GROUP BY director
    ORDER BY movie_count DESC
    LIMIT 20
""")
high_prod_directors.show(20, truncate=False)

print("\n[5.1续] 高分导演 TOP 20（平均评分最高，作品数 >= 5）- 有效评分视角")
high_avg_directors = spark.sql("""
    SELECT 
        TRIM(director) AS director,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating
    FROM (
        SELECT explode(directors_array) AS director, rating_score
        FROM valid_movies_with_dir
        WHERE directors_array IS NOT NULL
    )
    GROUP BY director
    HAVING COUNT(*) >= 5
    ORDER BY avg_rating DESC
    LIMIT 20
""")
high_avg_directors.show(20, truncate=False)

# 5.2 导演评分稳定性分析（标准差）
print("\n[5.2] 导演评分稳定性分析（作品数 >= 3，按标准差升序）- 有效评分视角")
director_stability = spark.sql("""
    SELECT 
        TRIM(director) AS director,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating,
        ROUND(STDDEV(rating_score), 2) AS rating_stddev
    FROM (
        SELECT explode(directors_array) AS director, rating_score
        FROM valid_movies_with_dir
        WHERE directors_array IS NOT NULL
    )
    GROUP BY director
    HAVING COUNT(*) >= 3
    ORDER BY rating_stddev ASC
    LIMIT 20
""")
director_stability.show(20, truncate=False)

print("\n" + "="*80)
print("A-2 统计分析全部完成（有效评分视角）")
print("="*80)



spark.stop()
