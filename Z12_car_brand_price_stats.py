from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, expr, row_number
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

# 初始化 SparkSession（启用 Hive 支持）
spark = SparkSession.builder \
    .appName("CarBrandPriceStats") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取原始 Hive 表
df = spark.table("car_data_hive")

# 类型转换
df = df.withColumn("new_price", col("new_price").cast(FloatType())) \
       .withColumn("current_price", col("current_price").cast(FloatType()))

# 去除无效记录（new_price 或 current_price 为 null 或 <= 0 或 NaN）
df_filtered = df.filter(
    (col("new_price").isNotNull()) & (col("new_price") > 0) &
    (col("current_price").isNotNull()) & (col("current_price") > 0)
)

# 聚合并计算价格降幅（%）
df_result = df_filtered.groupBy("brand").agg(
    avg("new_price").alias("avg_new_price"),
    avg("current_price").alias("avg_current_price"),
    expr("((avg(new_price) - avg(current_price)) / avg(new_price)) * 100").alias("price_drop_percent")
)

# 去除仍为 null 或 NaN 的行
df_result = df_result.filter(
    col("avg_new_price").isNotNull() & col("avg_current_price").isNotNull() & col("price_drop_percent").isNotNull()
)

# 添加递增 id 字段
window_spec = Window.orderBy("brand")
df_result = df_result.withColumn("id", row_number().over(window_spec))

# 创建 Hive 表（如果不存在）
spark.sql("""
    CREATE TABLE IF NOT EXISTS car_brand_price_stats (
        id INT COMMENT '主键 ID',
        brand STRING COMMENT '品牌',
        avg_new_price FLOAT COMMENT '平均新车价（万元）',
        avg_current_price FLOAT COMMENT '平均现价（万元）',
        price_drop_percent FLOAT COMMENT '平均降幅（百分比）'
    )
    STORED AS PARQUET
""")

# 写入 Hive 表（字段顺序一致）
df_result.select("id", "brand", "avg_new_price", "avg_current_price", "price_drop_percent") \
    .write.insertInto("car_brand_price_stats", overwrite=True)

print("✅ 成功将品牌价格降幅统计结果（含 id）存入 Hive 表 car_brand_price_stats")
