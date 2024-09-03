# coding:utf-8
from util.api_result_report import get_result_report
from util.send_mail import baymax_main
from new_api_cases.execute_indicator_cases import deal_request_method, cases_dir, CheckResult
from basic_info.setting import indicator_host, indicator_sheet, receivers_test, log, begin_times, pattern

if __name__ == '__main__':
    log.info("--------开始执行用例-------")
    log.info("开始时间：%s" % begin_times)

    """执行API用例并对比结果"""
    deal_request_method()
    CheckResult().deal_result()
    total_time = get_result_report(indicator_host, pattern[7])

    """发送邮件"""
    #baymax_main(cases_dir, indicator_host, receivers_test, indicator_sheet, begin_times, total_time)
