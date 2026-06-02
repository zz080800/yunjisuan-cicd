import pandas as pd
import time

print("[Pandas] 开始加载数据...")
start_load = time.time()
df = pd.read_csv('douban_movies.csv')   # 确保 csv 文件在同一目录
df['rating_score'] = pd.to_numeric(df['rating_score'], errors='coerce')
df['rating_count'] = pd.to_numeric(df['rating_count'], errors='coerce')
df['year'] = pd.to_numeric(df['year'], errors='coerce')
df_valid = df[
    (df['rating_score'] > 0) &
    (df['rating_count'] >= 100) &
    (df['year'].notna())
].copy()
print(f"[Pandas] 有效数据量: {len(df_valid)} 条, 加载过滤耗时: {time.time()-start_load:.2f}s")
start_query = time.time()
result = df_valid.groupby('year')['rating_score'].mean()
elapsed = time.time() - start_query
print(f"[Pandas] 聚合查询耗时: {elapsed:.4f} 秒")
print("[Pandas] 结果样例（前5年）:")
print(result.head())