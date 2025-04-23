from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

# === Step 1: 初始化 SparkSession（启用 Hive 支持）===
spark = SparkSession.builder \
    .appName("BrandAvgPriceStats") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

# === Step 2: 从 Hive 表读取数据 ===
df = spark.table("car_data_hive")

# === Step 3: 转换 current_price 为 float 类型 ===
df = df.withColumn("current_price_float", col("current_price").cast(FloatType()))

# === Step 4: 计算每个品牌的平均现价，按平均值降序排序 ===
avg_price_df = df.groupBy("brand") \
    .agg(avg("current_price_float").alias("avg_price")) \
    .orderBy(col("avg_price").desc())

# === Step 5: 添加 id 字段（使用 row_number 生成递增 id）===
window_spec = Window.orderBy(col("avg_price").desc())
avg_price_df = avg_price_df.withColumn("id", row_number().over(window_spec))

# === Step 6: 创建 Hive 表（如果不存在）===
spark.sql("""
    CREATE TABLE IF NOT EXISTS brand_avg_price (
        id INT COMMENT '主键ID',
        brand STRING COMMENT '品牌',
        avg_price FLOAT COMMENT '平均现价（万）'
    )
    STORED AS PARQUET
""")

# === Step 7: 写入数据到 Hive 表（覆盖原表）===
avg_price_df.select("id", "brand", "avg_price") \
    .write.insertInto("brand_avg_price", overwrite=True)

print("✅ 成功将品牌平均价格统计结果（含 id 字段）存入 Hive 表 brand_avg_price")
