import time

import requests


def failrun(n=3):
    def decorator(func):
        def wrapper(*args,**kw):
            for i in range(n):
                re= func(*args,**kw)
                if re.status_code==200:
                    return re
                else:
                    time.sleep(10)
        return wrapper
    return decorator


class Httpop:
    @failrun()
    def api_get(self,url,headers):
        res=requests.get(url=url, headers=headers)
        return res

    @failrun()
    def api_post(self,url,headers,data):
        res=requests.post(url=url, headers=headers,data=data)
        return res

