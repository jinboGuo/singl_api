# coding:utf-8
import requests
from basic_info.setting import MY_LOGIN_INFO2, MY_LOGIN_INFO_root,MY_LOGIN_INFO_dam,MY_LOGIN_INFO_root_dam
from util.format_res import dict_res
# admin账户登录，普通请求
# 获取登录后返回的X-AUTH-TOKEN
def get_auth_token(HOST):
    if '57' in HOST:
        res = requests.post(url=MY_LOGIN_INFO_dam["URL"], headers=MY_LOGIN_INFO_dam["HEADERS"], data=MY_LOGIN_INFO_dam["DATA"])
        dict_headers = dict(res.headers)
        token = dict_headers['X-AUTH-TOKEN']
        # print(token)
        return token
    else:
        res = requests.post(url=MY_LOGIN_INFO2["URL"], headers=MY_LOGIN_INFO2["HEADERS"], data=MY_LOGIN_INFO2["DATA"])
        # print(res.url)
        dict_headers = dict_res(res.text)
        # print(dict_headers)
        token = dict_headers['content']["accessToken"]
        # print(token)
        return 'Bearer ' + token


# 组装headers， 接口请求时调用
def get_headers(HOST):
    x_auth_token = get_auth_token(HOST)
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    # print(headers)
    return headers


# admin账户登录，POST请求中上传file文件，需要使用不同的content-type
# 组装headers， 接口请求时调用
def get_headers_upload(HOST):
    x_auth_token = get_auth_token(HOST)
    headers = {"X-AUTH-TOKEN": x_auth_token,
               'Origin': 'http://192.168.1.189:8515',
               'Referer': 'http://192.168.1.189:8515/',
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
               }
    # print(headers)
    return headers


# root用户登录使用
def get_auth_token_root(HOST):
    if '57' in HOST:
        res = requests.post(url=MY_LOGIN_INFO_dam["URL"], headers=MY_LOGIN_INFO_dam["HEADERS"],
                            data=MY_LOGIN_INFO_dam["DATA"])
        dict_headers = dict(res.headers)
        token = dict_headers['X-AUTH-TOKEN']
        # print(token)
        return token
    else:
        res = requests.post(url=MY_LOGIN_INFO_root["URL"], headers=MY_LOGIN_INFO_root["HEADERS"],
                            data=MY_LOGIN_INFO_root["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["accessToken"]
        # print(token)
        return 'Bearer ' + token

# 组装headers， 接口请求时调用
def get_headers_root(HOST):
    x_auth_token = get_auth_token_root(HOST)
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    # print(headers)
    return headers
#
# host = 'http://192.168.1.76:8515'
# print(get_auth_token(host))