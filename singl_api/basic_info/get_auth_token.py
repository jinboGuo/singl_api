# coding:utf-8
import requests
from basic_info.setting import MY_LOGIN_INFO_dsp_admin, \
    MY_LOGIN_INFO_dsp_customer, MY_LOGIN_INFO_compass, MY_LOGIN_INFO_dw, MY_LOGIN_INFO_DAM, MY_LOGIN_INFO_ROOT
from util.format_res import dict_res

def get_auth_token(HOST):
    """
    :param HOST:
    :return: 获取登录后返回的token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_DAM["URL"], headers=MY_LOGIN_INFO_DAM["HEADERS"],
                            data=MY_LOGIN_INFO_DAM["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except:
      return


def get_headers(HOST):
    """
    组装headers， 接口请求时调用
    :param HOST:
    :return:
    """
    try:
        Authorization = get_auth_token(HOST)
        headers = {'Content-Type': 'application/json', "Authorization": Authorization, "Accept": "application/json"}
        return headers
    except:
        return


def get_headers_upload(HOST):
    """
    :param HOST:
    :return: headers
    """
    try:
        x_auth_token = get_auth_token(HOST)
        headers = {"X-AUTH-TOKEN": x_auth_token,
                   'Origin': 'http://192.168.1.95:8515',
                   'Referer': 'http://192.168.1.95:8515/',
                   'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
                   }
        return headers
    except:
        return


def get_auth_token_root(HOST):
    """
    :param HOST:
    :return: token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_ROOT["URL"], headers=MY_LOGIN_INFO_ROOT["HEADERS"],
                            data=MY_LOGIN_INFO_ROOT["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except:
        return


def get_headers_root(HOST):
    """
    组装headers，接口请求时调用
    :param HOST:
    :return: headers
    """
    try:
        Authorization = get_auth_token_root(HOST)
        headers = {'Content-Type': 'application/json', "Authorization": Authorization, "Accept": "application/json"}
        return headers
    except:
        return

def get_auth_token_admin(HOST):
    """
    admin登录，返回access_token
    :param HOST:
    :return: token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_dsp_admin["URL"], headers=MY_LOGIN_INFO_dsp_admin["HEADERS"],
                            data=MY_LOGIN_INFO_dsp_admin["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except:
        return


def get_headers_admin(HOST):
    """
    组装headers， 接口请求时调用
    :param HOST:
    :return: headers
    """
    Authorization = get_auth_token_admin(HOST)
    headers = {'Content-Type': 'application/json', "Authorization": Authorization, "Accept": "application/json"}
    return headers


def get_auth_token_customer(HOST):
    """
    customer用户登录使用
    :param HOST:
    :return: token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_dsp_customer["URL"], headers=MY_LOGIN_INFO_dsp_customer["HEADERS"],
                            data=MY_LOGIN_INFO_dsp_customer["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except:
        return


def get_headers_customer(HOST):
    """
    组装headers， customer用户登录后，接口请求时调用
    :param HOST:
    :return:
    """
    try:
        Authorization = get_auth_token_customer(HOST)
        headers = {'Content-Type': 'application/json', "Authorization": Authorization, "Accept": "application/json"}
        return headers
    except:
        return



def get_auth_token_compass(HOST):
    """
    compass获取登录后返回的X-AUTH-TOKEN
    :param HOST:
    :return: token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_compass["URL"], headers=MY_LOGIN_INFO_compass["HEADERS"], json=MY_LOGIN_INFO_compass["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except:
        return


def get_headers_compass(HOST):
    """
    组装headers， compass用户登录后，接口请求时调用
    :param HOST:
    :return:
    """
    try:
        x_auth_token = get_auth_token_compass(HOST)
        headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
        return headers
    except:
        return

#
def get_auth_token_dw(HOST):
    """
    dw-assets获取登录后返回的access_token
    :param HOST:
    :return: access_token
    """
    try:
        res = requests.post(url=MY_LOGIN_INFO_dw["URL"], headers=MY_LOGIN_INFO_dw["HEADERS"],
                            data=MY_LOGIN_INFO_dw["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token
    except:
        return

def get_headers_dw(HOST):
    """
    组装headers， admin登录后，接口请求时调用
    :param HOST:
    :return: headers
    """
    try:
        Authorization = get_auth_token_dw(HOST)
        headers = {'Content-Type': 'application/json', "Authorization": Authorization, "Accept": "application/json"}
        return headers
    except:
        return