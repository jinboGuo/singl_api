# coding:utf-8
import requests
import unittest
from basic_info.get_auth_token import get_headers
from basic_info.url_info import query_component_status_url
from basic_info.setting import HOST_189


# 该脚本用来查询系统服务状态
class Check_status(unittest.TestCase):
    """查询系统服务状态接口测试"""
    def test_case01(self):
        """查询系统服务状态"""
        res = requests.get(url=query_component_status_url, headers=get_headers(HOST_189))
        print(res.text)
        # 检查响应状态码是否200
        self.assertEqual(res.status_code, 200, '系统服务状态接口响应状态码不是200，服务异常')



