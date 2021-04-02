from urllib.parse import urlparse

from basic_info.mylogging import myLog

log=myLog().getLog().logger
def get_host(url):
    try:
      #print(url)
      host = urlparse(url).scheme + '://' + urlparse(url).netloc
      #print("=================", host, type(host))
      return host
    except TypeError as e:
        log.error("%s: 返回值%s 格式无法转化为dict,原值返回" % (e, url))
        return ""


#url = 'http://192.168.1.57:8515/api/datasets/name/'
#print(get_host(url), type(get_host(url)))