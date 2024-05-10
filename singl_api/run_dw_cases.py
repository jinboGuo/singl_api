# coding:utf-8
import time
import unittest
from util.BeautifulReport import BeautifulReport
from util.api_result_report import get_result_report
from util.send_mail import baymax_main
from new_api_cases.execute_dw_cases import deal_request_method
from new_api_cases.execute_dw_cases import CheckResult
from basic_info.setting import dw_host, dw_sheet, receivers_test, log, receivers_list, begin_times, pattern

if __name__ == '__main__':
    log.info("--------开始执行用例-------")
    log.info("开始时间：%s" % begin_times)
    log.info("--------开始执行api case-------")

    """执行API用例并对比结果"""
    deal_request_method()
    CheckResult().deal_result()
    total_time = get_result_report(dw_host, pattern[2])

    """发送邮件"""
    baymax_main(dw_host, receivers_test, dw_sheet, begin_times,total_time)













