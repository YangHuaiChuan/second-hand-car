# 对于字段款式，去掉不含“款”字的记录；
# 对于字段表显里程，去掉内容里的“万”，显示在字段名中；
# 对于任意一条记录，如果它的任意字段为空值或者NaN，去掉这条记录。
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, regexp_replace,isnan
import pandas as pd
import os

os.environ["PYSPARK_PYTHON"] = "G:/课程报告或作业/大数据实训/second-hand-car/carvenv/Scripts/python.exe"

# 初始化 Spark 会话
spark = SparkSession.builder \
    .appName("UsedCarSecondCleaning") \
    .getOrCreate()

# 读取合并后的 Excel 文件
print("✅ 正在读取合并后的数据文件...")
merged_df = pd.read_excel("二手车数据_汽车之家_合并清洗版.xlsx")

# 将 pandas DataFrame 转换为 Spark DataFrame
sdf = spark.createDataFrame(merged_df)

# 1. 清洗款式字段，只保留包含“款”字的记录
print("🔄 正在清洗 '款式' 字段...")
sdf = sdf.filter(sdf["款式"].contains("款"))
print(f"✅ '款式' 字段处理完成，剩余 {sdf.count()} 条记录")

# 2. 清洗 '表显里程' 字段，去掉“万”字并重命名
print("🔄 正在清洗 '表显里程' 字段...")
sdf = sdf.withColumn("表显里程（公里）", regexp_replace("表显里程（公里）", "万", ""))
sdf = sdf.withColumnRenamed("表显里程（公里）", "表显里程（万公里）")
print(f"✅ '表显里程' 字段处理完成，剩余 {sdf.count()} 条记录")

print("🔄 正在删除包含空值或 NaN 的记录（保留电动车排量为空）...")

# 3.1. 保留电动车且排量为空的记录
electric_null_displacement = sdf.filter(
    (col("燃料类型") == "电动") & (col("排量（L）").isNull() | isnan("排量（L）"))
)

# 3.2. 剩余的记录（不满足上面条件的）做 dropna
rest = sdf.subtract(electric_null_displacement).dropna(how="any")

# 3.3. 合并保留的电动车记录和清洗后的其他记录
sdf = rest.unionByName(electric_null_displacement)

print(f"✅ 删除空值或 NaN 后，剩余 {sdf.count()} 条记录")

# 4. 将 DataFrame 转换回 pandas，并保存为新的 Excel 文件
print("🔄 正在转换为 pandas DataFrame 并保存为 Excel 文件...")
final_df = sdf.toPandas()

# 保存为 Excel
final_df.to_excel("二手车数据_汽车之家_第二次合并清洗版.xlsx", index=False)

print("\n✅ 数据已清洗并保存为：二手车数据_汽车之家_第二次合并清洗版.xlsx")

