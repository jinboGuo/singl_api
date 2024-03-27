import time
import requests

from basic_info.setting import log


def failrun(n=3):
    def decorator(func):
        def wrapper(*args, **kw):
            for i in range(n):
                re = func(*args, **kw)
                if str(re.status_code).startswith('2'):
                    return re
                else:
                    log.error("测试用例第%s次运行状态码%s,错误详情%s" % (i+1, re.status_code, re.text))
                    time.sleep(15)
        return wrapper
    return decorator


class Httpop:
    @failrun()
    def api_get(self,url,headers):
        res=requests.get(url=url, headers=headers)
        return res.text

    @failrun()
    def api_post(self,url,headers,data=None,json=None,files=None):
        res=requests.post(url=url, headers=headers,data=data,json=json,files=files)
        return res.text

