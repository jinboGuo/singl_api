from urllib.parse import urlparse

from basic_info.setting import log


def get_host(url):
    try:
      host = urlparse(url).scheme + '://' + urlparse(url).netloc
      return host
    except TypeError as e:
        log.error("%s: 返回值%s 格式无法转化为dict,原值返回" % (e, url))
        return ""