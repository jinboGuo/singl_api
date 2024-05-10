# coding:utf-8
from basic_info.setting import compass_host, compass_sheet, receivers_test, log, begin_times, pattern
from util.api_result_report import get_result_report
from util.send_mail import  baymax_main
from new_api_cases.execute_compass_cases import deal_request_method
from new_api_cases.execute_compass_cases import CheckResult

if __name__ == '__main__':
    log.info("--------开始执行用例-------")
    log.info("开始时间：%s" % begin_times)
    log.info("--------开始执行api case-------")

    """执行API用例并对比结果"""
    deal_request_method()
    CheckResult().deal_result()
    total_time = get_result_report(compass_host, pattern[3])

    """发送邮件"""
    baymax_main(compass_host, receivers_test, compass_sheet, begin_times,total_time)








