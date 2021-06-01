import time

import requests


def failrun(n=3):
    def decorator(func):
        def wrapper(*args,**kw):
            for i in range(n):
                re= func(*args,**kw)
                if str(re.status_code).startswith('2'):
                    return re
                else:
                    print("测试用例第%s次运行状态码%s"%(i+1,re.status_code))
                    time.sleep(10)
        return wrapper
    return decorator


class Httpop:
    @failrun()
    def api_get(self,url,headers):
        res=requests.get(url=url, headers=headers)
        return res

    @failrun()
    def api_post(self,url,headers,data=None,json=None,files=None):
        res=requests.post(url=url, headers=headers,data=data,json=json,files=files)
        return res

