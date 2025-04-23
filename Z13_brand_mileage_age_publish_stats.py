from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, round, avg, row_number
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.window import Window

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("BrandMileageAgePublishStats") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 Hive 表数据
df = spark.table("car_data_hive")

# 去掉上牌时间和发布时间不合理的记录
df = df.filter((col("reg_time").isNotNull()) & (col("post_time").isNotNull()))

# 解析时间格式（确保为 yyyy-MM-dd 格式）
df = df.withColumn("reg_date", col("reg_time").cast("date"))
df = df.withColumn("post_date", col("post_time").cast("date"))

# 计算车龄（年）和已发布天数
df = df.withColumn("car_age", (datediff(current_date(), col("reg_date")) / 365.0).cast(FloatType()))
df = df.withColumn("publish_days", datediff(current_date(), col("post_date")).cast(IntegerType()))

# 转换里程字段为 float，去除不合理的记录
df = df.withColumn("mileage_float", col("mileage").cast(FloatType()))
df = df.filter(df["mileage_float"].isNotNull())

# 分组统计
result = df.groupBy("brand", "mileage_float", "car_age") \
    .agg(round(avg("publish_days")).cast(IntegerType()).alias("avg_publish_days"))

# 添加 id 字段（自增）
window_spec = Window.orderBy("brand", "mileage_float", "car_age")
result = result.withColumn("id", row_number().over(window_spec))

# 创建 Hive 表（如果不存在）
spark.sql("""
    CREATE TABLE IF NOT EXISTS brand_mileage_age_publish_stats (
        id INT COMMENT '主键 ID',
        brand STRING COMMENT '品牌',
        mileage_float FLOAT COMMENT '里程数（万公里）',
        car_age FLOAT COMMENT '车龄（年）',
        avg_publish_days INT COMMENT '平均已发布天数'
    )
    STORED AS PARQUET
""")

# 写入 Hive 表（字段顺序匹配）
result.select("id", "brand", "mileage_float", "car_age", "avg_publish_days") \
    .write.insertInto("brand_mileage_age_publish_stats", overwrite=True)

print("✅ 成功将品牌-里程-车龄-发布天数统计数据（含 id）写入 Hive 表 brand_mileage_age_publish_stats")
