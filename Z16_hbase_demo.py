# from thrift.transport import TSocket,TTransport
# from thrift.protocol import TBinaryProtocol
# from hbase import Hbase

# # thrift默认端口是9090
# socket = TSocket.TSocket('121.43.62.42',9090)
# socket.setTimeout(5000)

# transport = TTransport.TBufferedTransport(socket)
# protocol = TBinaryProtocol.TBinaryProtocol(transport)

# client = Hbase.Client(protocol)
# socket.open()

# print(client.getTableNames())  # 获取当前所有的表名

import happybase

# 连接到 HBase Thrift 服务
connection = happybase.Connection(host='121.43.62.42', port=9090, timeout=5000)
connection.open()

# 打印所有表名
print("Table names:", connection.tables())

# 插入数据到 HBase
def insert_data():
    table = connection.table('test_table')  # 替换为你的 HBase 表名
    row_key = 'row1'
    data = {
        b'cf1:column1': b'value1',
        b'cf1:column2': b'value2'
    }
    table.put(row_key, data)
    print(f"✅ Inserted data into test_table with row key: {row_key}")

# 查询数据
def query_data():
    table = connection.table('test_table')  # 替换为你的 HBase 表名
    row_key = 'row1'

    row = table.row(row_key)
    print(f"Row key: {row_key}")
    for key, value in row.items():
        print(f"{key.decode('utf-8')}: {value.decode('utf-8')}")

# 插入并查询
insert_data()
query_data()

# 关闭连接
connection.close()

