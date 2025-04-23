from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# === Step 1: 初始化 SparkSession（启用 Hive 支持）===
spark = SparkSession.builder \
    .appName("BrandStats") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

# === Step 2: 查询 car_data_hive 表，统计每个品牌数量并降序排序 ===
query = """
    SELECT brand, COUNT(*) AS count
    FROM car_data_hive
    GROUP BY brand
    ORDER BY count DESC
"""
brand_stats_df = spark.sql(query)

# === Step 3: 添加递增 id 字段 ===
window_spec = Window.orderBy(brand_stats_df["count"].desc())
brand_stats_df = brand_stats_df.withColumn("id", row_number().over(window_spec))

# === Step 4: 创建 brand_stats 表（如果不存在）===
spark.sql("""
    CREATE TABLE IF NOT EXISTS brand_stats (
        id INT COMMENT '主键 ID',
        brand STRING COMMENT '品牌',
        count INT COMMENT '数量'
    )
    STORED AS PARQUET
""")

# === Step 5: 插入数据（字段顺序需与表结构一致）===
brand_stats_df.select("id", "brand", "count") \
    .write.insertInto("brand_stats", overwrite=True)

print("✅ 成功将品牌统计结果（含 id 字段）存入 Hive 表 brand_stats")
