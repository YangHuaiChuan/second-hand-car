from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# === Step 1: 初始化 SparkSession（启用 Hive 支持）===
spark = SparkSession.builder \
    .appName("FuelTypeStats") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

# === Step 2: 查询 car_data_hive 表，统计燃料类型为汽油和电动的数量 ===
query = """
    SELECT fuel_type, COUNT(*) AS count
    FROM car_data_hive
    WHERE fuel_type IN ('汽油', '电动')
    GROUP BY fuel_type
"""
fuel_stats_df = spark.sql(query)

# === Step 3: 添加 id 字段（递增）===
window_spec = Window.orderBy("fuel_type")  # 以 fuel_type 排序生成递增 id
fuel_stats_df = fuel_stats_df.withColumn("id", row_number().over(window_spec))

# === Step 4: 创建 fuel_type_stats 表（如果不存在）===
spark.sql("""
    CREATE TABLE IF NOT EXISTS fuel_type_stats (
        id INT,
        fuel_type STRING,
        count INT
    )
    STORED AS PARQUET
""")

# === Step 5: 插入统计结果到 fuel_type_stats 表中（覆盖原表）===
fuel_stats_df.select("id", "fuel_type", "count") \
    .write.insertInto("fuel_type_stats", overwrite=True)

print("✅ 成功将燃料类型统计结果（含 id 字段）存入 Hive 表 fuel_type_stats")
