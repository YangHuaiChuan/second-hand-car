from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

# 初始化 SparkSession（启用 Hive 支持）
spark = SparkSession.builder \
    .appName("CarBrandMileagePriceAvg") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取原始 Hive 表
df = spark.table("car_data_hive")

# 转换字段类型：价格为 float，里程数为 float
df = df.withColumn("price", col("current_price").cast(FloatType())) \
       .withColumn("mileage", col("mileage").cast(FloatType()))

# 去除价格或里程数无效的数据（价格 <= 0 或 里程数为空）
df = df.filter((col("price").isNotNull()) & (col("price") > 0) & (col("mileage").isNotNull()))

# 按品牌和里程数分组，计算平均价格
df_result = df.groupBy("brand", "mileage").agg(avg("price").alias("avg_price"))

# 添加递增 id 字段
window_spec = Window.orderBy("brand", "mileage")
df_result = df_result.withColumn("id", row_number().over(window_spec))

# 创建 Hive 表（如果不存在）
spark.sql("""
    CREATE TABLE IF NOT EXISTS car_brand_mileage_price_avg (
        id INT COMMENT '主键 ID',
        brand STRING COMMENT '品牌',
        mileage FLOAT COMMENT '里程数（万公里）',
        avg_price FLOAT COMMENT '平均价格（万元）'
    )
    STORED AS PARQUET
""")

# 写入 Hive 表（字段顺序一致）
df_result.select("id", "brand", "mileage", "avg_price") \
    .write.insertInto("car_brand_mileage_price_avg", overwrite=True)

print("✅ 成功将品牌、里程数和平均价格数据（含 id）写入 Hive 表 car_brand_mileage_price_avg")
