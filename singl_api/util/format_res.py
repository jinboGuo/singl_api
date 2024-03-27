# coding:utf-8
import json
from json.decoder import JSONDecodeError
import time
from basic_info.setting import log


# 将res从str转化为dict
def dict_res(res):
    if res:
        if isinstance(res, str):
            try:
                res = json.loads(res)
                return res
            except JSONDecodeError as e:
                log.error("%s: 返回值%s 格式无法转化为dict,原值返回" % (e, res))
                return res
        elif isinstance(res, dict):
            return res
        else:
            log.warn("返回值类型无法转化为dictionary")
            return res
    else:
        log.warn("返回值类型无法转化为dictionary")
        return None

def get_time():
    # 当前时间的时间戳转化为毫秒级
    time_stamp = int(time.time())*1000
    return time_stamp


