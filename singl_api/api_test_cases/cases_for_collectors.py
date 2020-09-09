# coding:utf-8
from basic_info.url_info import collector_table_url
from basic_info.setting import datasource_id,host
import unittest, requests
from basic_info.get_auth_token import get_headers


class ForCollector(unittest.TestCase):
    """验证limit值和total的关系"""
    def test_01(self):
        limit = 50
        par = {'id': datasource_id, 'limit': limit}
        response = requests.get(url=collector_table_url, headers=get_headers(host), params=par)
        response_json = response.json()
        total = response_json['total']
        content = response_json["content"]
        content_len = len(content)
        last = response_json["last"]
        # print(response.json())
        self.assertEqual(response.status_code, 200, '请求失败')
        if total > limit:
            self.assertEqual(last, False, 'total>limit时，last应该为false, 但是结果为true')
            self.assertEqual(content_len, limit, '返回的表数量和limit不一致')
        else:
            self.assertEqual(last, True, 'total <= limit时，last应该为True, 但是结果为false')

