# coding:utf-8
import requests
from basic_info.setting import MY_LOGIN_INFO_compass, MY_LOGIN_INFO_DAM, MY_LOGIN_INFO_ROOT, log
from util.format_res import dict_res

def get_auth_token():
    """
    :return: 获取登录后返回的token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_DAM["URL"], headers=MY_LOGIN_INFO_DAM["HEADERS"],
                            data=MY_LOGIN_INFO_DAM["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except Exception as e:
        log.error("执行过程中出错{}".format(e))


def get_headers():
    """
    组装headers， 接口请求时调用
    """
    try:
        authorization = get_auth_token()
        headers = {'Content-Type': 'application/json', "Authorization": authorization, "Accept": "application/json"}
        return headers
    except Exception as e:
        log.error("执行过程中出错{}".format(e))


def get_auth_token_root():
    """
    :return: token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_ROOT["URL"], headers=MY_LOGIN_INFO_ROOT["HEADERS"],
                            data=MY_LOGIN_INFO_ROOT["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except Exception as e:
        log.error("执行过程中出错{}".format(e))


def get_headers_root():
    """
    组装headers，接口请求时调用
    """
    try:
        authorization = get_auth_token_root()
        headers = {'Content-Type': 'application/json', "Authorization": authorization, "Accept": "application/json"}
        return headers
    except Exception as e:
        log.error("执行过程中出错{}".format(e))



def get_auth_token_compass():
    """
    compass获取登录后返回的X-AUTH-TOKEN
    :return: token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_compass["URL"], headers=MY_LOGIN_INFO_compass["HEADERS"], json=MY_LOGIN_INFO_compass["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except Exception as e:
        log.error("执行过程中出错{}".format(e))


def get_headers_compass():
    """
    组装headers， compass用户登录后，接口请求时调用
    :return:
    """
    try:
        x_auth_token = get_auth_token_compass()
        headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
        return headers
    except Exception as e:
        log.error("执行过程中出错{}".format(e))