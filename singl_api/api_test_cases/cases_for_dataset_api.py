# coding:utf-8
from basic_info.get_auth_token import get_headers
from new_api_cases.get_statementId import get_tenant
from util.data_from_db import get_datasource, schema
import unittest
import requests
import json
import time
from basic_info.setting import MySQL_CONFIG, host
from util.Open_DB import MYSQL

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


class CreateDataSet(unittest.TestCase):
    """该脚本用来测试create dataset api"""
    from basic_info.url_info import create_dataset_url
    storage = get_datasource()
    storageConfigurations = {"format": "csv", "path": "/tmp/gubingjie", "relativePath": "/tmp/gubingjie",
                             "recursive": "false", "header": "false", "separator": ",", "quoteChar": "\"",
                             "escapeChar": "\\"}

    dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'

    def test_case01(self):
        """--正常创建DBdataset，选择已存在的schema属性为true--"""
        try:
            schema_query = 'select id from merce_schema where name = "gbj_schema"'
            schema = ms.ExecuQuery(schema_query)
        except Exception as e:
            raise e
        else:
            schema_id = {}
            schema_id["id"] = schema[0]["id"]
            # print(schema_id)
            data = {"name": self.dataset_name, "expiredPeriod": 0, "storage": "JDBC", "storageConfigurations": self.storage, "schema": schema_id, "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
            res = requests.post(url=self.create_dataset_url, headers=get_headers(host), data=json.dumps(data))

            # print(res.status_code, res.text)
            self.assertEqual(res.status_code, 201, 'DB-dataset创建失败')
            # time.sleep(2)

    def test_case02(self):
        """--正常创建HDFSdataset--"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'HDFSdataset'
        schema_info = schema()  # data_from_db中schema()查询schema
        data = {"name": dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
        "storageConfigurations": self.storageConfigurations, "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(host), data=json.dumps(data))
        # 断言成功时响应状态码为201
        print("开始创建hdfs")
        print(res.status_code, res.text)
        self.assertEqual(res.status_code, 201, 'HDFS dataset创建失败')
        text = json.loads(res.text)
        text = text["id"]
        try:
            query = 'select id from merce_dataset where name = "%s"' % dataset_name
            new_dataset = ms.ExecuQuery(query)
        except:
            print('没有查询到datasetname为%s的dataset' % dataset_name)
        else:
            new_dataset = new_dataset[0]["id"]
            # print(text, new_dataset)
            # 根据datasetname查询到dataset ID， 并和返回的text中包含的ID进行对比
            self.assertEqual(text, new_dataset, '返回的dataset id和使用该dataset的 name查询出的id不相等')


class Get_DataSet(unittest.TestCase):
    """该脚本用来测试dataset查询接口"""

    def test_case01(self):
        """使用id查询"""
        try:
            dataset_sql = 'select id, name from merce_dataset order by create_time desc limit 1'
            dataset_info = ms.ExecuQuery(dataset_sql)
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            # print(type(dataset_id[0][0]))
        except Exception as e:
            raise e
        else:
            url2 = '%s/api/datasets/%s?tenant=%s' % (host, dataset_id, get_tenant(host))
            response = requests.get(url=url2, headers=get_headers(host)).text
            response = json.loads(response)
            response_id = response["id"]
            response_name = response["name"]
            # print("id:", response["id"])
            # print({"id": dataset_id, "name": dataset_name} == {"id": response_id, "name": response_name})
            self.assertEqual({"id": dataset_id, "name": dataset_name}, {"id": response_id, "name": response_name}, '两次查询得到的dataset id和name不一致，查询失败')


