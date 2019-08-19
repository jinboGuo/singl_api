# coding:utf-8
from basic_info.get_auth_token import get_headers
import requests,json

# 根据flowid查询需要更新流程所需的flow_body
def get_flow_update_body():
    from basic_info.url_info import flow_update_flowid_url
    res = requests.get(url=flow_update_flowid_url, headers=get_headers())
    # print(res.text)
    return json.loads(res.text)


get_flow_update_body()
