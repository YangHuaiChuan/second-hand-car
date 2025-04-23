import os
import pandas as pd
from pyspark.sql import SparkSession

# === Step 1: 从 HDFS 下载 Excel 文件到本地 ===
hdfs_path = "/user/yanghc/二手车数据_汽车之家_第二次合并清洗版.xlsx"
local_excel = "/tmp/car_data_temp.xlsx"  # 避免中文路径乱码问题

# 下载文件
os.system(f"hadoop fs -get '{hdfs_path}' '{local_excel}'")

# === Step 2: 用 pandas 读取 Excel 文件 ===
df_pd = pd.read_excel(local_excel)

# === Step 3: 初始化 Spark Session（启用 Hive 支持）===
spark = SparkSession.builder \
    .appName("ExcelToHiveFromHDFS") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

# === Step 4: pandas → Spark DataFrame ===
df_spark = spark.createDataFrame(df_pd)

# 注册为临时视图
df_spark.createOrReplaceTempView("temp_car_data")

# === Step 5: 创建 Hive 表（带字段注释）===
spark.sql("""
    CREATE TABLE IF NOT EXISTS car_data_hive (
        brand STRING COMMENT '品牌',
        style STRING COMMENT '款式',
        model STRING COMMENT '型号',
        current_price STRING COMMENT '现价（万）',
        new_price STRING COMMENT '新车价格（万）',
        fuel_type STRING COMMENT '燃料类型',
        reg_time STRING COMMENT '上牌时间',
        mileage STRING COMMENT '表显里程（万公里）',
        gearbox STRING COMMENT '变速箱',
        displacement STRING COMMENT '排量（L）',
        post_time STRING COMMENT '发布时间',
        inspect_due STRING COMMENT '年检到期',
        insurance_due STRING COMMENT '保险到期',
        transfer_count STRING COMMENT '过户次数',
        location STRING COMMENT '所在地',
        engine STRING COMMENT '发动机',
        horsepower STRING COMMENT '马力',
        level STRING COMMENT '车辆级别',
        color STRING COMMENT '车身颜色',
        drive_mode STRING COMMENT '驱动方式',
        province STRING COMMENT '省份'
    )
    STORED AS PARQUET
""")

# === Step 6: 插入数据 ===
# 为避免表结构冲突，推荐使用 INSERT INTO
spark.sql("INSERT INTO TABLE car_data_hive SELECT * FROM temp_car_data")

print("✅ 成功将 HDFS 上的 Excel 文件导入 Hive 表 car_data_hive")
