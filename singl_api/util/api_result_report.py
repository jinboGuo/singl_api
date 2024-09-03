import unittest
from util.BeautifulReport import BeautifulReport
from basic_info.setting import log, begin_times
import time
from util.timestamp_13 import change_time


def get_result_report(host, pattern):
    """
    通过该类defaultTestLoader下面的discover()方法
    可自动更具测试目录start_dir匹配查找测试用例文件（test*.py），
    并将查找到的测试用例组装到测试套件
    host是执行环境
    pattern是指执行用例的py名称，比如execute_cases.py
    """
    test_suite = unittest.defaultTestLoader.discover('new_api_cases', pattern=pattern)
    result = BeautifulReport(test_suite)
    result.report(filename='api_cases_report', description=host, report_dir='Reports', theme='theme_cyan')
    stop_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log.info("结束时间：%s" % stop_time)
    total_time = int(time.time()) - int(time.mktime(time.strptime(begin_times, '%Y-%m-%d %H:%M:%S')))
    total_time = change_time(total_time)
    log.info("总耗时长：%s" % total_time)
    return total_time
