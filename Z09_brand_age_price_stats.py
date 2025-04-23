from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, avg, row_number
from pyspark.sql.types import FloatType, DateType
from pyspark.sql.window import Window

# 初始化 SparkSession（启用 Hive 支持）
spark = SparkSession.builder \
    .appName("CarBrandAgePriceAvg") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 Hive 中原始数据表
df = spark.table("car_data_hive")

# 将 reg_time 转为日期格式，current_price 转为 float
df = df.withColumn("reg_date", col("reg_time").cast(DateType())) \
       .withColumn("price", col("current_price").cast(FloatType()))

# 去除无效数据：reg_time 或 price 为空，或价格 <= 0
df = df.filter((col("reg_date").isNotNull()) & (col("price").isNotNull()) & (col("price") > 0))

# 计算车龄（年），单位为 float
df = df.withColumn("car_age", (datediff(current_date(), col("reg_date")) / 365.0).cast(FloatType()))

# 只保留品牌、车龄、价格字段
df_filtered = df.select("brand", "car_age", "price")

# 按品牌和车龄分组，计算平均价格
df_result = df_filtered.groupBy("brand", "car_age").agg(avg("price").alias("avg_price"))

# 添加递增 id 字段
window_spec = Window.orderBy("brand", "car_age")
df_result = df_result.withColumn("id", row_number().over(window_spec))

# 创建 Hive 表（如果不存在）
spark.sql("""
    CREATE TABLE IF NOT EXISTS car_brand_age_price_avg (
        id INT COMMENT '主键 ID',
        brand STRING COMMENT '品牌',
        car_age FLOAT COMMENT '车龄（年）',
        avg_price FLOAT COMMENT '平均价格（万）'
    )
    STORED AS PARQUET
""")

# 写入 Hive 表（字段顺序与表结构一致）
df_result.select("id", "brand", "car_age", "avg_price") \
    .write.insertInto("car_brand_age_price_avg", overwrite=True)

print("✅ 品牌-车龄 平均价格数据（含 id）已成功写入 Hive 表 car_brand_age_price_avg")
