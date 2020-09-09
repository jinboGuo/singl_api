# coding:utf-8
import unittest
from basic_info.setting import MY_LOGIN_INFO2,host
import requests
import json
from util.encrypt import encrypt_rf



class CheckLogin(unittest.TestCase):

    """login 接口测试"""
    """测试登录接口"""
    def setUp(self):
        self.login_info = MY_LOGIN_INFO2["DATA"]
        self.error_name = MY_LOGIN_INFO2["DATA_ERROR_NAME"]
        # print(self.error_name)
        self.login_header = MY_LOGIN_INFO2["HEADERS"]

    def test_case01(self):
        """正常登录"""
        from basic_info.url_info import login_url
        res = requests.post(url=login_url, headers=self.login_header, data=self.login_info)
        print(res.headers)
        print('----------------')
        print(res.text)
        self.assertEqual(res.status_code, 200)
        # assert res.status_code == 200

    def test_case02(self):
        """用户名错误"""
        from basic_info.url_info import login_url
        res = requests.post(url=login_url, headers=self.login_header, data=self.error_name)
        # print(res.text)
        assert res.status_code == 400
        self.assertIn('wrong userName/password', res.text,  '用户名错误时断言失败')


    def test_case03(self):
        """密码错误"""
        from basic_info.url_info import login_url
        data = {'name': encrypt_rf('admin'), 'password': encrypt_rf('12345678'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
        res = requests.post(url=login_url, headers=self.login_header, data=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        self.assertIn('wrong userName/password', res.text,  '密码错误时断言失败')
    #
    def test_case04(self):
        """租户错误"""
        from basic_info.url_info import login_url
        data = {'name': encrypt_rf('admin'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('defaul99')}
        # print(data)
        res = requests.post(url=login_url, headers=self.login_header, data=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        self.assertEqual({"err": "the tenant defaul99 can not found"}, json.loads(res.text), '租户错误时断言失败')

    def test_case05(self):
        """用户名为空"""
        from basic_info.url_info import login_url
        data = {'name': encrypt_rf(''), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
        res = requests.post(url=login_url, headers=self.login_header, data=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err": "both the parameters name,password and tenant can not be null."}

    def test_case06(self):
        """密码为空"""
        from basic_info.url_info import login_url
        data = {'name': encrypt_rf('admin'), 'password': encrypt_rf(''), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
        res = requests.post(url=login_url, headers=self.login_header, data=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err":"both the parameters name,password and tenant can not be null."}
    #
    def test_case07(self):
        """租户为空"""
        from basic_info.url_info import login_url
        data = {'name': encrypt_rf('admin'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('')}
        res = requests.post(url=login_url, headers=self.login_header, data=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err":"both the parameters name,password and tenant can not be null."}

