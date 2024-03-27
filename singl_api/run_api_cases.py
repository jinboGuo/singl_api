# coding:utf-8
import unittest
from BeautifulReport import BeautifulReport
from util.send_mail import baymax_main
import datetime
from new_api_cases.execute_cases import deal_request_method
from new_api_cases.execute_cases import CheckResult
from basic_info.setting import host, baymax_sheet, receivers_test, receivers_list, log

if __name__ == '__main__':
    """
    通过该类defaultTestLoader下面的discover()方法
    可自动更具测试目录start_dir匹配查找测试用例文件（test*.py），
    并将查找到的测试用例组装到测试套件
    """
    log.info("--------开始执行用例-------")
    start_time = datetime.datetime.now()
    log.info("开始时间：%s" % start_time)
    log.info("--------开始执行api case-------")

    """执行API用例并对比结果"""
    deal_request_method()
    CheckResult().deal_result()
    test_suite = unittest.defaultTestLoader.discover('new_api_cases', pattern='execute_cases.py')
    result = BeautifulReport(test_suite)
    result.report(filename='api_result', description='自动化测试结果汇总', report_dir='Reports',theme='theme_cyan')
    stop_time = datetime.datetime.now()
    log.info("结束时间：%s" % stop_time)
    total_time = stop_time - start_time
    log.info("总耗时：%s" % total_time)

    """发送邮件"""
    baymax_main(host, receivers_test, baymax_sheet, start_time,total_time)