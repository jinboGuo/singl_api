API用例模板说明																
![image](https://github.com/jinboGuo/singl_api/assets/58332140/80b4a950-4f78-42e0-8ab1-c90ab7a7e721)

详细说明：																
key_word		"create:创建数据时，涉及请求方法POST     query:查询时,涉及请求方法POST,GET
update:更新数据时,涉及请求方法PUT       delete:删除数据时（例：/datasource/removeList）,涉及请求方法POST,DELETE"														
text_relation		"预期内容和实际返回内容的关系，目前处理了等于和包含两种关系：   = ，in
如果expect_text和response_text相等，请写 = 。举例：删除数据的接口，返回内容为空，expect_text和response_text列都为空，text_relation为=，即可判断二者关系
如果expect_text包含在response_text中，请写 in。举例：创建dataset时，返回内容包含name等其他数据，"														
parameters		1.请求无需参数时：parameter列中不需要填写任何内容														
		"2.请求有参数时：
URL中需要有参数时，参数位置使用{}占位，对应的参数按顺序写在 parameter中，多个参数之间使用&&分隔。
例如：put接口的参数支持：查询请求体参数#查询接口地址 
     post接口的参数支持：请求体参数#数据源名称 、请求体参数#数据源名称#元数据名称
URL中无参数时，将接口请求使用的参数写在parameter列中，可以为SQL语句、{}形式、[]形式，说明如下：

**参数以 { 开头，默认整个字符串直接作为接口传递参数，里面包含根目录id时，需要把根目录id 1093986570292658176值修改为：数据源目录、数据集目录、元数据目录、数据计算目录、采集机目录、数据采集目录、数据存储目录、任务视图目录、数据资产目录、数据共享目录、数据安全目录、文件编目目录、数据标准目录。包含数据集id时，需要把id值改成：数据集主键；包含数据集名称时，需要把值修改为：数据集名称；包含元数据id时，需要把id值改成：元数据主键；包含元数据名称时，需要把值修改为：元数据名称；包含数据源id时，需要把id值改成：数据源主键；包含数据源名称时，需要把值修改为：数据源名称；
例如：""name"":""gjb_type_mysql_dss随机数"",""type"":""DB"",""description"":""gjb_mysql"",""resourceId"":""数据源目录"",""attributes"":{""DBType"":""Mysql"",""directConnection"":true,""collectorId"":"""",""collectorName"":"""",""urls"":""jdbc:mysql://192.168.1.82:3306/auto_apitest"",""database"":""auto_apitest"",""tags"":[],""tagNames"":[],""user"":""merce"",""password"":""merce"",""caseSensitive"":""false"",""tableQuote"":""\"""",""name"":""mysql-connector-j-8.0.32-internal-cluster1"",""driver"":""com.mysql.cj.jdbc.Driver"",""url"":""jdbc:mysql://192.168.1.82:3306/auto_apitest"",""defaultUrl"":""jdbc:mysql://[HOST]:[PORT]/[DB]"",""port"":""3306"",""paraPrefix"":""?"",""paraSep"":""&"",""jarPath"":""mysql-connector-j-8.0.32-internal-cluster1.jar"",""endpoint"":""jdbc:mysql://192.168.1.82:3306/auto_apitest""},""status"":1}

{""fieldGroup"":{""type"":""FieldGroup"",""group"":true,""andOr"":""AND"",""fields"":[{""type"":""Field"",""group"":false,""andOr"":""AND"",""name"":""parentId"",""oper"":""EQUAL"",""value"":[""数据源目录""]}]},""ordSort"":[{""name"":""lastModifiedTime"",""order"":""DESC""}],""pageable"":{""pageNum"":1,""pageSize"":20,""pageable"":true}}

**参数以 select开头，默认为 查询语句，需要先执行数据库查询，拿到返回值并组装成接口使用参数。
例如：删除最新修改的1条数据源
select id from merce_dss ORDER BY last_modified_time desc limit 3

**参数以 [ 开头，默认给定的参数为list，转化后作为接口传参。												
																
																
																
																
																
![image](https://github.com/jinboGuo/singl_api/assets/58332140/9a4ccdad-ef3c-407c-9c5f-666c9ff8662f)
