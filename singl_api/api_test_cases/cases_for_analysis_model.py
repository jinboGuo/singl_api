# coding:utf-8
from basic_info.get_auth_token import get_headers
import unittest, time, requests, random
from basic_info.setting import preProcessFlowId, preProcessFlowName, processDataId
from util.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG,  HOST_189
from util.format_res import get_time
from util.timestamp_13 import timestamp_to_13

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


class CasesForRule(unittest.TestCase):
    from basic_info.url_info import create_rule_url
    def test_create_rule_EL(self):
        """创建Custom - EL规则"""
        rule_name = "rule_for_EL_students_copy" + str(random.randint(0, 9999))
        data = {"aggType": "None",
                "buildType": "Custom",  # NOTE: 没有该参数时可以创建成功
                "customType": "EL",   # NOTE: 没有该参数时可以创建成功
                "dataScope": "Field",
                "fieldValueType": "Any",
                "ruleOption": {"paramsMap": {}},
                "name": rule_name,
                "customValue": "/\\d{1,3}/"  # NOTE: 没有该参数时可以创建成功
                }
        response = requests.post(url=self.create_rule_url, headers=get_headers(), json=data)
        self.assertEqual(201,response.status_code, '创建规则接口调用失败, 失败原因%s' %response.text)
        self.assertIsNotNone(response.json()["id"], '规则创建成功后未返回规则id')

    def test_create_rule_SQL(self):
        """创建Custom - SQL规则"""
        rule_name = "rule_for_SQL_students_copy" + str(random.randint(0, 999999999))
        data = {"aggType":"None",
                "buildType":"Custom",
                "customType":"SQL",
                "dataScope":"Field",
                "fieldValueType":"Number",
                "ruleOption":{"paramsMap":{}},
                # "id":"57dbfa51-31af-4dc1-9a7e-7ba50c322ef2",
                "ruleClass":"",
                "name":rule_name,
                "customValue":"$grade > 80"
                }
        response = requests.post(url=self.create_rule_url, headers=get_headers(), json=data)
        self.assertEqual(201, response.status_code, '创建规则接口调用失败，失败原因%s' %response.text)
        self.assertIsNotNone(response.json()["id"], '规则创建成功后未返回规则id')
        return response.json()["id"]


class QueryRule(unittest.TestCase):
    from basic_info.url_info import rule_query_url
    def test_query_rule_all(self):
        """查询全部规则"""
        data = {
                "fieldList": [],
                "sortObject": {
                    "field": "lastModifiedTime",
                    "orderDirection": "DESC"
                },
                "offset": 0,
                "limit": 10
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code,  '查询全部规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '全部规则查询返回结果为空')

    def test_query_rule_buildIn(self):
        """查询内建规则"""
        data = {"fieldList": [{"fieldName":"buildType", "fieldValue":"Builtin", "comparatorOperator":"EQUAL"}],
                "sortObject": {"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code, '查询内建规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '内建规则查询结果为空')

    def test_query_rule_extend(self):
        """查询extend规则"""
        data = {"fieldList":[{"fieldName":"customType","fieldValue":"Extend","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code, '查询extend规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], 'extend规则查询结果为空')

    def test_query_rule_EL(self):
        """查询表达式规则"""
        data = {"fieldList":[{"fieldName":"customType","fieldValue":"EL","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code,  '表达式规则查询失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '表达式规则查询结果为空')

    def test_query_rule_SQL(self):
        """查询SQL规则"""
        data = {"fieldList":[{"fieldName":"customType","fieldValue":"SQL","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code, 'SQL规则查询失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], 'SQL规则查询结果为空')

    def test_query_rule_KeyWords(self):
        """根据关键字students查询规则"""
        data = {"fieldList":[{"fieldName":"name","fieldValue":"%students%","comparatorOperator":"LIKE"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code,  'SQL规则查询失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], 'SQL规则查询结果为空')
        contents = response.json()["content"]
        # print(contents,'\n' , type(contents))
        for content in contents:
            # print(content["name"])
            self.assertIn('students', content["name"], '按照关键字查询规则时，返回的查询结果中，name未包含查询关键字')

    def test_query_rule_time(self):
        """根据时间段查询规则 2019-1-1/1-24"""
        begin_time = 1546272000000
        end_time = 1548259200000
        data = {"fieldList":[{"fieldName":"lastModifiedTime","fieldValue":begin_time,"comparatorOperator":"GREATER_THAN"},
                             {"fieldName":"lastModifiedTime","fieldValue":end_time,"comparatorOperator":"LESS_THAN"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(HOST_189), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code, '按照修改时间查询规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '按照修改时间查询规则结果为空')
        contents = response.json()["content"]
        # print(contents,'\n' , type(contents))
        count = 0
        for content in contents:
            # print(content["lastModifiedTime"])
            count += 1
            print(count, content["lastModifiedTime"], content["lastModifiedTime"])
            self.assertGreaterEqual(content["lastModifiedTime"], begin_time, '按照lastModifiedTime查询规则时，返回的查询结果中，lastModifiedTime不大于开始时间')
            self.assertGreaterEqual(end_time, content["lastModifiedTime"], '按照lastModifiedTime查询规则时，返回的查询结果中，lastModifiedTime不小于结束时间')

    def test_query_rule_NumberType_Any(self):
        """根据数据类型_Any查询规则"""
        data = {"fieldList":[{"fieldName":"fieldValueType","fieldValue":"Any","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(HOST_189), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code, '根据数据类型_Any查询规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '根据数据类型_Any查询规则查询结果为空')

    def test_query_rule_NumberType_Number(self):
        """根据数据类型_number查询规则"""
        data = {"fieldList":[{"fieldName":"fieldValueType","fieldValue":"Number","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(HOST_189), json=data)
        # print(response.status_code, response.json())
        self.assertEqual( 200, response.status_code,'根据数据类型_Number查询规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '根据数据类型_Number查询规则查询结果为空')

    def test_query_rule_NumberType_String(self):
        """根据数据类型_string查询规则"""
        data = {"fieldList":[{"fieldName":"fieldValueType","fieldValue":"String","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(HOST_189), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code, '根据数据类型_String查询规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '根据数据类型_String查询规则查询结果为空')

    def test_query_rule_NumberType_Date(self):
        """根据数据类型_date查询规则"""
        data = {"fieldList":[{"fieldName":"fieldValueType","fieldValue":"Date","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(HOST_189), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code, '根据数据类型_Date查询规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '根据数据类型_Date查询规则查询结果为空')

    def test_query_rule_NumberType_Other(self):
        """根据数据类型_other查询规则"""
        data = {"fieldList":[{"fieldName":"fieldValueType","fieldValue":"Other","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},
                "offset":0,
                "limit":8
                }
        response = requests.post(url=self.rule_query_url, headers=get_headers(HOST_189), json=data)
        # print(response.status_code, response.json())
        self.assertEqual(200, response.status_code, '根据数据类型_Other查询规则失败,失败原因%s' % response.text)
        if response.json()["totalElements"]:
            self.assertIsNotNone(response.json()["content"], '根据数据类型_Other查询规则查询结果为空')


class QueryAndUpdateRuleDetail(unittest.TestCase):
    # rule id
    sql_rule_id = CasesForRule().test_create_rule_SQL()
    query_rule_detail_url = "%s/api/woven/rule/%s" % (HOST_189, sql_rule_id)

    def test_rule_detail(self):
        """查询规则详细信息：最新一条sql规则"""
        response = requests.get(url=self.query_rule_detail_url, headers=get_headers(HOST_189))
        # print(response.status_code, response.url, response.json())
        self.assertEqual(200, response.status_code, '查询规则详情接口调用失败,失败原因%s' % response.text)
        self.assertEqual(self.sql_rule_id, response.json()["id"], '查询规则不一致')
        return response.json()["name"]

    def test_rule_update(self):
        """更新规则-SQL类型的name"""
        data = {"id": self.sql_rule_id,
                "name": self.test_rule_detail() + '_after_update' + str(random.randint(0,99999)),
                "creator":"admin",
                "createTime":1548389901000,
                "lastModifier":"admin",
                "lastModifiedTime": get_time(),
                "owner":"2059750c-a300-4b64-84a6-e8b086dbfd42",
                "version":1,
                "moduleVersion":0,
                "enabled":1,
                "tenant":{"id":"2d7ad891-41c5-4fba-9ff2-03aef3c729e5","name":"default","creator":"root","createTime":1532942318000,"lastModifier":"f8aff341-9303-4135-b393-1d322e4638e2","lastModifiedTime":1544078372000,"owner":"f8aff341-9303-4135-b393-1d322e4638e2","version":0,"moduleVersion":0,"enabled":1,"resourceQueues":["default","merce.normal"],"hdfsSpaceQuota":0,"zid":"","expiredPeriod":0},
                "buildType":"Custom",
                "customType":"SQL",
                "ruleClass":"com.merce.woven.app.metadata.rule.RuleCustomSQLValidation",
                "customValue":"$grade > 70",
                "priority":1,
                "aggType":"None",
                "dataScope":"FieldsCombination",
                "fieldValueType":"Any",
                "ruleOption":{"paramsMap":{"inputGroup":[{"name":"customSqlValidation","vtype":"string","defaultValue":"","displayStr":"自定义 SQL 表达式","required":True}],
                                           "outputGroup":[{"name":"outputFields","vtype":"MultiField","defaultValue":"*","displayStr":"输出字段","required":True},
                                                          {"name":"qualityType","vtype":"string","defaultValue":"normal","displayStr":"打分方式","required":True,"valueOptions":["normal","ignore"]},
                                                          {"name":"outputLimit","vtype":"BigInt","defaultValue":"1000000","displayStr":"坏数据行数限制","required":False}]
                                           },
                              "outValueType":"Any",
                              "outputFields":[]
                              },
                "regex":".*",
                "regexFlag":0,
                "expiredPeriod":0
                }
        response = requests.put(url=self.query_rule_detail_url, headers=get_headers(HOST_189), json=data)
        self.assertEqual(204, response.status_code, '更新规则接口调用失败,失败原因%s' % response.text)


class RemoveRule(unittest.TestCase):
    def test_rule_removeList(self):
        """批量删除分析规则"""
        from basic_info.url_info import rule_removeList_url
        rule_id_list_sql = 'select id from merce_zrule where build_type = "Custom" and custom_type = "SQL" and ' \
                  'name like "rule_for_SQL_students_copy%"  order by create_time desc limit 2 '
        rule_id_list = ms.ExecuQuery(rule_id_list_sql)
        data = [item[key] for item in rule_id_list for key in item]
        # print(data)
        if data:
            response = requests.post(url=rule_removeList_url, headers=get_headers(HOST_189), json=data)
            self.assertEqual(200, response.status_code, '删除规则接口调用失败,失败原因%s' % response.text)


class CreateAnalysisModel(unittest.TestCase):
    """
    创建分析模板
    """
    from basic_info.url_info import create_analysis_model

    def test_create_analysis_model1(self):
        """创建分析模板, 不添加flow"""
        model_name = 'api_test_analysis_model' + str(random.randint(0, 99999))
        data = {"name": model_name,
                # "preProcessFlowId": preProcessFlowId,
                # "preProcessFlowName": preProcessFlowName,
                "processDataId": processDataId,
                "processDataType": "Dataset"
                }
        response = requests.post(url=self.create_analysis_model, headers=get_headers(HOST_189), json=data)
        self.assertEqual(201, response.status_code, '分析模板创建失败,失败原因%s' % response.text)
        self.assertIsNotNone(response.json(), '分析模板创建后返回None')
        return response.json()["id"]

    def test_Add_Model_Rule_For_Analysis_model(self):
        """新建模板添加模板规则"""
        rule_name = "model-rule" + str(timestamp_to_13(digits=13))
        modelId = self.test_create_analysis_model1()
        data = {"name": rule_name,
                "modelId": modelId,
                "dataId": "*",
                "ruleId": CasesForRule().test_create_rule_SQL(),  # rule id 后续改为创建rule后返回
                # "ruleName": "digit format",
                "priority": 1,
                "inputParams":
                    {"outputGroup": {"0": {"name": "outputFields", "value": "*"},
                                    "1": {"name": "qualityType", "value": "normal"},
                                    "2": {"name": "outputLimit", "value": "1000000"}},
               "inputGroup":{"0": {"name": "inputELColumns", "value": "*"},
                             "1": {"name": "customExpression", "value": "/\\d/"}}}}

        url = "%s/api/woven/zmodrules/%s" % (HOST_189, modelId)
        response = requests.post(url=url, headers=get_headers(HOST_189), json=data)
        self.assertEqual(201, response.status_code, '分析模板规则添加失败,失败原因%s' % response.text)

    def test_create_analysis_model2(self):
        """创建分析模板, 添加flow"""
        model_name = 'api_test_analysis_model' + str(random.randint(0, 99999))
        data = {"name": model_name,
                "preProcessFlowId": preProcessFlowId,
                "preProcessFlowName": preProcessFlowName,
                "processDataId": processDataId,
                "processDataType": "Dataset"
                }
        response = requests.post(url=self.create_analysis_model, headers=get_headers(HOST_189), json=data)
        self.assertEqual(201, response.status_code, '分析模板创建失败,失败原因%s' % response.text)
        self.assertIsNotNone(response.json(), '分析模板创建后返回None')
        time.sleep(5)

    def test_delete_analysis_model(self):
        """删除分析规则"""
        from basic_info.url_info import zmod_removeList_url
        sql = 'select id from merce_model where name like "api_test_analysis_model%" ORDER BY create_time desc limit 1'
        model_id_info = ms.ExecuQuery(sql)
        # model_id = model_id_info[0]["id"]
        # data = []
        # data.append(model_id)
        data = [item[key] for item in model_id_info for key in item]
        response = requests.post(url=zmod_removeList_url, headers=get_headers(HOST_189), json=data)
        self.assertEqual(204, response.status_code, '删除分析模板失败,失败原因%s' % response.text)
        # print(response.status_code)

class QueryAnalysisRule(unittest.TestCase):
    def test_query_analysis_rule(self):
        """查看单个模板的分析规则"""
        query_zmod_rule = "%s/api/woven/zmodrules/%s/detailslist" % (HOST_189, CreateAnalysisModel().test_create_analysis_model1())
        data = {"fieldList": [], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
        response = requests.post(url=query_zmod_rule, headers=get_headers(HOST_189), json=data)
        self.assertEqual(200, response.status_code, '查询分析模板规则的接口调用失败,失败原因%s' % response.text)



if __name__ == '__main__':
    unittest.main()