from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, percentile_approx, max, min, row_number
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

# 初始化 SparkSession（启用 Hive 支持）
spark = SparkSession.builder \
    .appName("CarBrandTransferPriceStats") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取原始 Hive 表
df = spark.table("car_data_hive")

# 转换字段类型：价格为 float，过户次数为 int
df = df.withColumn("price", col("current_price").cast(FloatType())) \
       .withColumn("transfer_count", col("transfer_count").cast("int"))

# 去除价格或过户次数无效的数据（空或价格 <= 0）
df = df.filter((col("price").isNotNull()) & (col("price") > 0) & (col("transfer_count").isNotNull()))

# 统计价格分布：min、Q1、median、Q3、max
df_result = df.groupBy("brand", "transfer_count").agg(
    min("price").alias("min_price"),
    expr("percentile_approx(price, 0.25)").alias("q1_price"),
    expr("percentile_approx(price, 0.5)").alias("median_price"),
    expr("percentile_approx(price, 0.75)").alias("q3_price"),
    max("price").alias("max_price")
)

# 添加递增 id 字段
window_spec = Window.orderBy("brand", "transfer_count")
df_result = df_result.withColumn("id", row_number().over(window_spec))

# 创建 Hive 表（如果不存在）
spark.sql("""
    CREATE TABLE IF NOT EXISTS car_brand_transfer_price_stats (
        id INT COMMENT '主键 ID',
        brand STRING COMMENT '品牌',
        transfer_count INT COMMENT '过户次数',
        min_price FLOAT COMMENT '最低价格',
        q1_price FLOAT COMMENT '第一四分位数',
        median_price FLOAT COMMENT '中位数',
        q3_price FLOAT COMMENT '第三四分位数',
        max_price FLOAT COMMENT '最高价格'
    )
    STORED AS PARQUET
""")

# 写入 Hive 表（字段顺序保持一致）
df_result.select("id", "brand", "transfer_count", "min_price", "q1_price", "median_price", "q3_price", "max_price") \
    .write.insertInto("car_brand_transfer_price_stats", overwrite=True)

print("✅ 成功将价格分布统计数据（含 id）写入 Hive 表 car_brand_transfer_price_stats")
