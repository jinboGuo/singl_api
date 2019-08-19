# coding:utf-8
from basic_info.get_auth_token import get_headers
import unittest
import requests
from util.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, zmod_id,HOST_189

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


class CasesForZmod(unittest.TestCase):
    from basic_info.url_info import query_zdaf
    """分析任务信息接口"""
    def test_create_Zdaf_flow(self):
        """创建分析任务-flow"""
        from basic_info.url_info import create_zmod_flow_url
        # data = ["e2dbfd88-0e2d-4fa2-b145-75c1a13ab455"]  # 分析模板id
        response = requests.post(url=create_zmod_flow_url, headers=get_headers(HOST_189), json=zmod_id)
        self.assertEqual(200, response.status_code, '分析任务创建失败')
        self.assertEqual(zmod_id[0], response.json()["modelId"], "分析任务的modelId不一致")

    def test_query_zdaf_all(self):
        """查询所有的分析任务"""
        from basic_info.url_info import query_zdaf
        data = {"fieldList": [], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0,
                "limit": 8}
        response = requests.post(url=query_zdaf, headers=get_headers(HOST_189), json=data)
        # 接口查询返回8条分析任务id并排序
        content_ids = []
        for id in response.json()["content"]:
            content_ids.append(id["id"])
        content_ids.sort()
        self.assertEqual(200, response.status_code, '分析任务查询接口调用失败')
        # 数据库查询得到最新的8条分析任务id并排序
        zdaf_data_limit8 = 'select id from merce_zdaf where flow_status != "PREPARING" order by last_modified_time desc limit 8'
        zdaf8 = ms.ExecuQuery(zdaf_data_limit8)
        zdaf_ids = [item[key] for item in zdaf8 for key in item]
        zdaf_ids.sort()
        self.assertEqual(content_ids, zdaf_ids, '分析任务查询接口返回的查询结果和数据库数据不一致')

    def test_query_zdaf_by_name(self):
        """按照分析模板名称查询分析任务"""
        data = {"fieldList":[{"fieldName":"name","fieldValue":"%api_test_use%","comparatorOperator":"LIKE"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        keywords = 'api_test_use'
        response = requests.post(url=self.query_zdaf, headers=get_headers(HOST_189), json=data)
        self.assertEqual(200, response.status_code, '分析任务查询接口调用失败')
        if response.json()["content"]:
            self.assertIn(keywords, response.json()["content"][0]["name"], '查询得到的分析任务name中没有包含查询关键字keyword')

    def test_query_zdaf_by_time(self):
        """根据时间段查询规则 2019-1-1/1-24"""
        begin_time = 1546272000000
        end_time = 1548777599000
        data = {"fieldList":[{"fieldName":"lastModifiedTime","fieldValue": begin_time,"comparatorOperator":"GREATER_THAN"},
                             {"fieldName":"lastModifiedTime","fieldValue": end_time,"comparatorOperator":"LESS_THAN"}
                             ],
                "sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        response = requests.post(url=self.query_zdaf, headers=get_headers(HOST_189), json=data)
        self.assertEqual(200, response.status_code, 'SQL规则查询失败')
        if response.json()["content"]:
            self.assertIsNotNone(response.json()["content"], '分析任务查询结果为空')
        # print(contents,'\n' , type(contents))
        for content in response.json()["content"]:
            # print(content["lastModifiedTime"])
            self.assertGreaterEqual(content["lastModifiedTime"], begin_time,
                                    '按照lastModifiedTime查询规则时，返回的查询结果中，lastModifiedTime未包含在查询时间段内')
            self.assertGreaterEqual(end_time, content["lastModifiedTime"],
                                    '按照lastModifiedTime查询规则时，返回的查询结果中，lastModifiedTime未包含在查询时间段内')

    def test_query_zmod_model_detail(self):
        """查看任务关联模板详情"""
        from basic_info.url_info import query_zmod_model_detail_url

        response = requests.get(url=query_zmod_model_detail_url, headers=get_headers(HOST_189))
        self.assertEqual(200, response.status_code, '查看任务关联模板详情接口调用失败')
        self.assertEqual(zmod_id[0], response.json()["id"], '任务详情查询结果中id不一致')

    def test_query_zmod_exectuion(self):
        """查看任务执行信息"""
        from basic_info.url_info import query_zmod_exectuion_url
        response = requests.get(url=query_zmod_exectuion_url, headers=get_headers(HOST_189))
        self.assertEqual(200, response.status_code, '查看任务执行信息接口调用失败')


class QueryZmodResult(unittest.TestCase):
    """质量分析结果统计"""
    def test_zdaf_result(self):
        """查看质量分析统计结果：包含质量评级，坏数据占比, 统计方式为总计"""
        from basic_info.url_info import query_zdaf_result_url
        data = {"fieldList":[{"fieldName":"flowStatus","fieldValue":"SUCCEEDED","comparatorOperator":"EQUAL"}],
                "sortObject":{"field":"createTime","orderDirection":"ASC"},"offset":0,"limit":10}
        response = requests.post(url=query_zdaf_result_url, headers=get_headers(HOST_189), json=data)
        self.assertEqual(200, response.status_code, '查询评估结果统计接口调用失败')
        print(response.status_code, response.json())


if __name__ == '__main__':
    unittest.main()