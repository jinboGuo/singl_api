from util.encrypt import parameter_ungzip
from basic_info.setting import MySQL_CONFIG
from util.Open_DB import MYSQL

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

sql = "select links from merce_flow where  name = 'minus_0628_3_no_element_test'"
result = ms.ExecuQuery(sql)
end_result = result[0]["links"]
print(result[0]["links"])
print(parameter_ungzip(end_result))