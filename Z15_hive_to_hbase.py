from pyspark.sql import SparkSession
import happybase

# HBase 客户端连接设置（使用 HappyBase）
def create_happybase_connection():
    connection = happybase.Connection(host='121.43.62.42', port=9090)
    connection.open()
    return connection

# 上传数据到 HBase
def upload_to_hbase(connection, table_name, hive_data):
    column_family = "cf"  # HBase 表的列族名称
    table = connection.table(table_name)

    for row in hive_data.collect():  # collect() 获取所有行
        row_key = str(row['id']) if row['id'] is not None else "unknown_key"
        print(f"Uploading to HBase with row_key: {row_key}")

        data = {}

        if table_name == "brand_avg_price":
            data[f"{column_family}:brand"] = str(row['brand'])
            data[f"{column_family}:avg_price"] = str(row['avg_price'])
        elif table_name == "brand_mileage_age_publish_stats":
            data[f"{column_family}:brand"] = str(row['brand'])
            data[f"{column_family}:mileage_float"] = str(row['mileage_float'])
            data[f"{column_family}:car_age"] = str(row['car_age'])
            data[f"{column_family}:avg_publish_days"] = str(row['avg_publish_days'])
        elif table_name == "brand_stats":
            data[f"{column_family}:brand"] = str(row['brand'])
            data[f"{column_family}:count"] = str(row['count'])
        elif table_name == "car_brand_age_price_avg":
            data[f"{column_family}:brand"] = str(row['brand'])
            data[f"{column_family}:car_age"] = str(row['car_age'])
            data[f"{column_family}:avg_price"] = str(row['avg_price'])
        elif table_name == "car_brand_mileage_price_avg":
            data[f"{column_family}:brand"] = str(row['brand'])
            data[f"{column_family}:mileage"] = str(row['mileage'])
            data[f"{column_family}:avg_price"] = str(row['avg_price'])
        elif table_name == "car_brand_price_stats":
            data[f"{column_family}:brand"] = str(row['brand'])
            data[f"{column_family}:avg_new_price"] = str(row['avg_new_price'])
            data[f"{column_family}:avg_current_price"] = str(row['avg_current_price'])
            data[f"{column_family}:price_drop_percent"] = str(row['price_drop_percent'])
        elif table_name == "car_brand_transfer_price_stats":
            data[f"{column_family}:brand"] = str(row['brand'])
            data[f"{column_family}:transfer_count"] = str(row['transfer_count'])
            data[f"{column_family}:min_price"] = str(row['min_price'])
            data[f"{column_family}:q1_price"] = str(row['q1_price'])
            data[f"{column_family}:median_price"] = str(row['median_price'])
            data[f"{column_family}:q3_price"] = str(row['q3_price'])
            data[f"{column_family}:max_price"] = str(row['max_price'])
        elif table_name == "fuel_type_stats":
            data[f"{column_family}:fuel_type"] = str(row['fuel_type'])
            data[f"{column_family}:count"] = str(row['count'])

        try:
            table.put(row_key, data)
        except Exception as e:
            print(f"❌ Error uploading row with row_key {row_key}: {e}")

# 主函数
def main():
    spark = SparkSession.builder \
        .appName("HiveToHBase_HappyBase") \
        .enableHiveSupport() \
        .getOrCreate()

    connection = create_happybase_connection()

    hbase_table_names = [
        # "brand_avg_price",
        # "brand_mileage_age_publish_stats",
        # "brand_stats",
        # "car_brand_age_price_avg",
        "car_brand_mileage_price_avg",
        "car_brand_price_stats",
        "car_brand_transfer_price_stats",
        "fuel_type_stats"
    ]

    for table_name in hbase_table_names:
        print(f"🔄 正在处理表: {table_name}")
        hive_data = spark.table(table_name)
        upload_to_hbase(connection, table_name, hive_data)
        print(f"✅ 成功将 {table_name} 数据存入 HBase 表")

    connection.close()

if __name__ == "__main__":
    main()
