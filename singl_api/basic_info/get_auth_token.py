# coding:utf-8
import requests
from basic_info.setting import MY_LOGIN_INFO_root, MY_LOGIN_INFO_dam, MY_LOGIN_INFO_dsp_admin, \
    MY_LOGIN_INFO_dsp_customer, MY_LOGIN_INFO_compass, MY_LOGIN_INFO_dw
from util.format_res import dict_res

def get_auth_token(HOST):
    """
    :param HOST:
    :return: 获取登录后返回的X-AUTH-TOKEN
    """
    try:
        if '57' in HOST:
            res = requests.post(url=MY_LOGIN_INFO_dam["URL"], headers=MY_LOGIN_INFO_dam["HEADERS"], data=MY_LOGIN_INFO_dam["DATA"])
            dict_headers = dict(res.headers)
            token = dict_headers['X-AUTH-TOKEN']
            return token
        else:
            res = requests.post(url=MY_LOGIN_INFO_root["URL"], headers=MY_LOGIN_INFO_root["HEADERS"],
                                data=MY_LOGIN_INFO_root["DATA"])
            dict_headers = dict_res(res.text)
            token = dict_headers['content']["access_token"]
            return 'Bearer ' + token
    except:
      return


def get_headers(HOST):
    """
    组装headers， 接口请求时调用
    :param HOST:
    :return:
    """
    Authorization = get_auth_token(HOST)
    headers = {'Content-Type': 'application/json', "Authorization": Authorization, "Accept": "application/json"}
    return headers


def get_headers_upload(HOST):
    """
    :param HOST:
    :return: headers
    """
    x_auth_token = get_auth_token(HOST)
    headers = {"X-AUTH-TOKEN": x_auth_token,
               'Origin': 'http://192.168.1.62:8515',
               'Referer': 'http://192.168.1.62:8515/',
               'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
               }
    return headers


def get_auth_token_root(HOST):
    """
    :param HOST:
    :return: token
    """
    if '57' in HOST:
        res = requests.post(url=MY_LOGIN_INFO_dam["URL"], headers=MY_LOGIN_INFO_dam["HEADERS"],
                            data=MY_LOGIN_INFO_dam["DATA"])
        dict_headers = dict(res.headers)
        token = dict_headers['X-AUTH-TOKEN']
        return token
    else:
        try:
            res = requests.post(url=MY_LOGIN_INFO_root["URL"], headers=MY_LOGIN_INFO_root["HEADERS"],
                                data=MY_LOGIN_INFO_root["DATA"])
            dict_headers = dict_res(res.text)
            token = dict_headers['content']["access_token"]
            return 'Bearer ' + token
        except:
            return


def get_headers_root(HOST):
    """
    组装headers，接口请求时调用
    :param HOST:
    :return: headers
    """
    x_auth_token = get_auth_token_root(HOST)
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "*/*"}
    return headers


def get_auth_token_admin(HOST):
    """
    admin登录，返回access_token
    :param HOST:
    :return: token
    """
    if '57' in HOST:
        res = requests.post(url=MY_LOGIN_INFO_dsp_admin["URL"], headers=MY_LOGIN_INFO_dsp_admin["HEADERS"],
                            data=MY_LOGIN_INFO_dsp_admin["DATA"])
        dict_headers = dict(res.headers)
        token = dict_headers['Authorization']
        return token
    else:
        res = requests.post(url=MY_LOGIN_INFO_dsp_admin["URL"], headers=MY_LOGIN_INFO_dsp_admin["HEADERS"],
                            data=MY_LOGIN_INFO_dsp_admin["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return 'Bearer ' + token


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
    if '57' in HOST:
        res = requests.post(url=MY_LOGIN_INFO_dsp_customer["URL"], headers=MY_LOGIN_INFO_dsp_customer["HEADERS"],
                            data=MY_LOGIN_INFO_dsp_customer["DATA"])
        dict_headers = dict(res.headers)
        token = dict_headers['Authorization']
        return token
    else:
        res = requests.post(url=MY_LOGIN_INFO_dsp_customer["URL"], headers=MY_LOGIN_INFO_dsp_customer["HEADERS"],
                            data=MY_LOGIN_INFO_dsp_customer["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return 'Bearer ' + token


def get_headers_customer(HOST):
    """
    组装headers， customer用户登录后，接口请求时调用
    :param HOST:
    :return:
    """
    Authorization = get_auth_token_customer(HOST)
    headers = {'Content-Type': 'application/json', "Authorization": Authorization, "Accept": "application/json"}
    return headers


def get_auth_token_compass(HOST):
    """
    compass获取登录后返回的X-AUTH-TOKEN
    :param HOST:
    :return: token
    """
    if '57' in HOST:
        res = requests.post(url=MY_LOGIN_INFO_compass["URL"], headers=MY_LOGIN_INFO_compass["HEADERS"],
                            data=MY_LOGIN_INFO_compass["DATA"])
        dict_headers = dict(res.headers)
        token = dict_headers['Authorization']
        return token
    else:
        res = requests.post(url=MY_LOGIN_INFO_compass["URL"], headers=MY_LOGIN_INFO_compass["HEADERS"], json=MY_LOGIN_INFO_compass["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return token


def get_headers_compass(HOST):
    """
    组装headers， compass用户登录后，接口请求时调用
    :param HOST:
    :return:
    """
    x_auth_token = get_auth_token_compass(HOST)
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    return headers

#
def get_auth_token_dw(HOST):
    """
    dw-assets获取登录后返回的access_token
    :param HOST:
    :return: access_token
    """
    if '57' in HOST:
        res = requests.post(url=MY_LOGIN_INFO_dw["URL"], headers=MY_LOGIN_INFO_dw["HEADERS"],
                            data=MY_LOGIN_INFO_dw["DATA"])
        dict_headers = dict(res.headers)
        token = dict_headers['Authorization']
        return token
    else:
        res = requests.post(url=MY_LOGIN_INFO_dw["URL"], headers=MY_LOGIN_INFO_dw["HEADERS"],
                            data=MY_LOGIN_INFO_dw["DATA"])
        dict_headers = dict_res(res.text)
        token = dict_headers['content']["access_token"]
        return 'Bearer ' + token


def get_headers_dw(HOST):
    """
    组装headers， admin登录后，接口请求时调用
    :param HOST:
    :return: headers
    """
    Authorization = get_auth_token_dw(HOST)
    headers = {'Content-Type': 'application/json', "Authorization": Authorization, "Accept": "application/json"}
    return headers