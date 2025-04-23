# 处理规则：处理每张表时，现价和新车价格字段名带上单位万，字段值去掉万；
# 表显里程字段名加上单位公里，字段内容去掉公里；排量字段名加上L，字段内容去掉L；上
# 牌时间修改格式为xxxx-xx；过户次数只显示数值，比如0，1等；
# 发动机的字段内容按照空格拆分，要第二个和第三个元素，第二个如190马力变为新的字段名为马力，数值为190，第三个元素仍然属于发动机，如L4，第一个元素如2.5L不要；
# 车辆级别的内容如果为空就去掉这条记录。

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, col, when, trim
import os

os.environ["PYSPARK_PYTHON"] = "G:/课程报告或作业/大数据实训/second-hand-car/carvenv/Scripts/python.exe"

# 初始化 Spark 会话
spark = SparkSession.builder \
    .appName("UsedCarMerge") \
    .getOrCreate()

folder_path = "./"  # 所有 Excel 所在目录
file_list = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]

merged_df = None

for file_name in file_list:
    province = file_name.split("_")[2]
    file_path = os.path.join(folder_path, file_name)
    
    # 用 pandas 读 Excel，然后转成 Spark DF（PySpark 原生不支持 xlsx）
    import pandas as pd
    pdf = pd.read_excel(file_path)
    pdf['省份'] = province
    sdf = spark.createDataFrame(pdf)

    # 清洗字段内容（去单位）
    sdf = sdf.withColumn("现价（万）", regexp_replace("现价", "万", "")) \
             .withColumn("新车价格（万）", regexp_replace("新车价格", "万", "")) \
             .withColumn("表显里程（公里）", regexp_replace("表显里程", "公里", "")) \
             .withColumn("排量（L）", regexp_replace("排量", "L", ""))

    # 上牌时间格式调整：2024年01月 → 2024-01
    sdf = sdf.withColumn("上牌时间", regexp_replace(regexp_replace("上牌时间", "年", "-"), "月", ""))

    # 过户次数提取数字
    sdf = sdf.withColumn("过户次数", regexp_replace("过户次数", r"[^0-9]", ""))

    # 发动机拆分：提取“马力”和“L4”
    sdf = sdf.withColumn("马力", regexp_replace(split("发动机", " ").getItem(1), "马力", "")) \
             .withColumn("发动机", split("发动机", " ").getItem(2))

    # 删除“车辆级别”为空的记录
    sdf = sdf.filter(trim(col("车辆级别")) != "")

    # 打印提示信息（收集后）
    for row in sdf.select("品牌", "型号", "省份").collect():
        print(f"已处理：{row['品牌']} - {row['型号']} - {row['省份']}")

    # 合并
    merged_df = sdf if merged_df is None else merged_df.unionByName(sdf)

# 最终选择的列
columns_to_keep = [
    '品牌', '款式', '型号', '现价（万）', '新车价格（万）', '燃料类型', '上牌时间',
    '表显里程（公里）', '变速箱', '排量（L）', '发布时间', '年检到期', '保险到期',
    '过户次数', '所在地', '发动机', '马力', '车辆级别', '车身颜色', '驱动方式', '省份'
]
merged_df = merged_df.select(*columns_to_keep)

# 保存结果（保存为 CSV，或另用 pandas 保存为 Excel）
merged_df.toPandas().to_excel("二手车数据_汽车之家_合并清洗版.xlsx", index=False)
print("\n✅ 所有数据已合并并清洗完成，保存为 Excel")