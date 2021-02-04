from time import sleep
import sys
from basic_info.setting import Compass_MySQL_CONFIG, Compass_scheduler
from util.Open_DB import MYSQL
from util.conn_linux import Linux

ms = MYSQL(Compass_MySQL_CONFIG["HOST"], Compass_MySQL_CONFIG["USER"], Compass_MySQL_CONFIG["PASSWORD"], Compass_MySQL_CONFIG["DB"])
host = Linux(Compass_scheduler["HOST"], Compass_scheduler["USER"], Compass_scheduler["PASSWORD"])
# 监控kafka消息
def check_s_l_message(data):
    print("----开始监控发送kafka消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
    message_select_result = ms.ExecuQuery(data.encode('utf-8'))
    while 1:
        if message_select_result:
            print("----s_l_message----存在kafka消息数据！", "line: ", str(sys._getframe().f_lineno),\
                  "in ", sys._getframe().f_code.co_name)
            print("full_name: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number: %s"\
              % (message_select_result[0]["full_name"], message_select_result[0]["status"],\
                 message_select_result[0]["slice_type"], str(message_select_result[0]["slice_time"]),\
                 str(message_select_result[0]["create_time"]), str(message_select_result[0]["row_number"])))
            sleep(5)
            message_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if message_select_result:
                sleep(5)
                continue
            else:
                print("----s_l_message----kafka消息数据被删除！", "line: ", str(sys._getframe().f_lineno),\
                      "in ", sys._getframe().f_code.co_name)
                print("----完成监控发送kafka消息----", "line: ", str(sys._getframe().f_lineno),\
                      "in ", sys._getframe().f_code.co_name)
                break
        else:
            message_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("----s_l_message----没有任务数据！", "line: ", str(sys._getframe().f_lineno),\
                  "in ", sys._getframe().f_code.co_name)
            sleep(5)

# 监控日志任务调度
def check_s_r_task(data):
    print("----开始监控日志任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
    task_select_result = ms.ExecuQuery(data.encode('utf-8'))
    while 1:
        if task_select_result:
            if task_select_result[0]["status"] == 0:
                print("----s_r_task----日志任务待执行！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("task_oid: %s job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number: %s dataset_name: %s dataformat_name: %s"\
                      % (str(task_select_result[0]["task_oid"]), str(task_select_result[0]["job_oid"]),\
                         task_select_result[0]["status"], task_select_result[0]["slice_type"],\
                         str(task_select_result[0]["slice_time"]), str(task_select_result[0]["create_time"]),\
                         task_select_result[0]["row_number"], task_select_result[0]["cdo_name"], task_select_result[0]["dataformat_name"]))
                task_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if task_select_result[0]["status"] == 0:
                    sleep(5)
                    continue
                elif task_select_result[0]["status"] == 1:
                    print("----s_r_task----日志任务执行中！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                    print("task_oid: %s job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number: %s dataset_name: %s dataformat_name: %s" \
                       % (str(task_select_result[0]["task_oid"]), str(task_select_result[0]["job_oid"]),\
                         task_select_result[0]["status"], task_select_result[0]["slice_type"],\
                         str(task_select_result[0]["slice_time"]), str(task_select_result[0]["create_time"]),\
                         task_select_result[0]["row_number"], task_select_result[0]["cdo_name"], task_select_result[0]["dataformat_name"]))
                    task_select_result = ms.ExecuQuery(data.encode('utf-8'))
                    try:
                        if task_select_result:
                            if task_select_result[0]["status"] == 1:
                                sleep(5)
                                continue
                        else:
                            if len(task_select_result) == 0:
                                print("----s_r_task----日志任务数据被删除！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                                print("----完成监控日志任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                                break
                    except:
                        return
            elif task_select_result[0]["status"] == 1:
                print("----s_r_task----日志任务执行中！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("task_oid: %s job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number: %s dataset_name: %s dataformat_name: %s" \
                      % (str(task_select_result[0]["task_oid"]), str(task_select_result[0]["job_oid"]),\
                         task_select_result[0]["status"], task_select_result[0]["slice_type"],\
                         str(task_select_result[0]["slice_time"]), str(task_select_result[0]["create_time"]),\
                         task_select_result[0]["row_number"], task_select_result[0]["cdo_name"], task_select_result[0]["dataformat_name"]))
                task_select_result = ms.ExecuQuery(data.encode('utf-8'))
                try:
                    if task_select_result:
                        if task_select_result[0]["status"] == 1:
                            sleep(5)
                            continue
                    else:
                        if len(task_select_result) == 0:
                            print("----s_r_task----日志任务数据被删除！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                            print("----完成监控日志任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                            break
                except:
                    return
            else:
                if len(task_select_result) == 0:
                    print("----s_r_task----日志任务数据被删除！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                    print("----完成监控日志任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                    break

        else:
            task_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("----s_r_task----没有日志任务数据！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
            sleep(5)

# 监控任务执行
def check_s_l_result_task(data):
    print("----开始监控任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
    result_task_select_result = ms.ExecuQuery(data.encode('utf-8'))
    while 1:
        if result_task_select_result:
            if result_task_select_result[0]["status"] == 1:
                print("----s_l_result_task----任务执行中！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s"\
                      % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"],\
                         result_task_select_result[0]["slice_type"], str(result_task_select_result[0]["slice_time"]),\
                         str(result_task_select_result[0]["create_time"]), result_task_select_result[0]["row_number_output"],\
                         result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                result_task_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if result_task_select_result:
                    if result_task_select_result[0]["status"] == 1:
                        sleep(5)
                        continue
                    elif result_task_select_result[0]["status"] == 2:
                        print("----s_l_result_task----任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s" \
                              % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"],\
                                 result_task_select_result[0]["slice_type"], str(result_task_select_result[0]["slice_time"]),\
                                 str(result_task_select_result[0]["create_time"]), result_task_select_result[0]["row_number_output"],\
                                 result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                        print("----完成监控任务调度消息----")
                        break
                    elif result_task_select_result[0]["status"] == -2:
                        print("----s_l_result_task----任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s" \
                              % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"],\
                                 result_task_select_result[0]["slice_type"], str(result_task_select_result[0]["slice_time"]),\
                                 str(result_task_select_result[0]["create_time"]), result_task_select_result[0]["row_number_output"],\
                                 result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                        print("----完成监控任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        break
                else:
                    return
            elif result_task_select_result[0]["status"] == 2:
                print("----s_l_result_task----任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s"\
                      % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"],\
                         result_task_select_result[0]["slice_type"], str(result_task_select_result[0]["slice_time"]),\
                         str(result_task_select_result[0]["create_time"]), result_task_select_result[0]["row_number_output"],\
                         result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                print("----完成监控任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            elif result_task_select_result[0]["status"] == -2:
                print("----s_l_result_task----任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s"\
                      % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"],\
                         result_task_select_result[0]["slice_type"], str(result_task_select_result[0]["slice_time"]),\
                         str(result_task_select_result[0]["create_time"]), result_task_select_result[0]["row_number_output"],\
                         result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                print("----完成监控任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            else:
                print("----完成监控任务调度消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break

        else:
            result_task_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("----s_l_result_task----没有任务数据！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
            sleep(5)

# 监控任务执行结果输入
def check_s_l_result_detail(data):
    print("----开始监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
    result_detail_select_result = ms.ExecuQuery(data.encode('utf-8'))
    n = 0
    while 1:
        if result_detail_select_result:
            if result_detail_select_result[0]["status"] == 1:
                print("----s_l_result_detail----输入任务执行中！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"],\
                         result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]),\
                         str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"],\
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"],\
                         result_detail_select_result[0]["dataformat_name"]))
                sleep(5)
                result_detail_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if result_detail_select_result:
                    if result_detail_select_result[0]["status"] == 1:
                        continue
                    elif result_detail_select_result[0]["status"] == 2:
                        print("----s_l_result_detail----输入任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                              % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], \
                                 result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]), \
                                 str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"], \
                                 result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], \
                                 result_detail_select_result[0]["dataformat_name"]))
                        print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        break
                    elif result_detail_select_result[0]["status"] == 3:
                        print("----s_l_result_detail----输入任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                              % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], \
                                 result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]), \
                                 str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"], \
                                 result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], \
                                 result_detail_select_result[0]["dataformat_name"]))
                        print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        break
                    elif result_detail_select_result[0]["status"] == -2:
                        print("----s_l_result_detail----输入任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                              % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], \
                                 result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]), \
                                 str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"], \
                                 result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], \
                                 result_detail_select_result[0]["dataformat_name"]))
                        print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        break
                    elif result_detail_select_result[0]["status"] == -3:
                        print("----s_l_result_detail----输入任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                        print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                              % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], \
                                 result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]), \
                                 str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"], \
                                 result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], \
                                 result_detail_select_result[0]["dataformat_name"]))
                        print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno),\
                              "in ", sys._getframe().f_code.co_name)
                        break
                    else:
                        print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno),\
                              "in ", sys._getframe().f_code.co_name)
                        break
                else:
                    print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                    break
            elif result_detail_select_result[0]["status"] == 2:
                print("----s_l_result_detail----输入任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], \
                         result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]), \
                         str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"], \
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], \
                         result_detail_select_result[0]["dataformat_name"]))
                print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            elif result_detail_select_result[0]["status"] == 3:
                print("----s_l_result_detail----输入任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], \
                         result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]), \
                         str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"], \
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], \
                         result_detail_select_result[0]["dataformat_name"]))
                print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            elif result_detail_select_result[0]["status"] == -2:
                print("----s_l_result_detail----输入任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], \
                         result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]), \
                         str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"], \
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], \
                         result_detail_select_result[0]["dataformat_name"]))
                print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            elif result_detail_select_result[0]["status"] == -3:
                print("----s_l_result_detail----输入任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], \
                         result_detail_select_result[0]["slice_type"], str(result_detail_select_result[0]["slice_time"]), \
                         str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"], \
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], \
                         result_detail_select_result[0]["dataformat_name"]))
                print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno),\
                      "in ", sys._getframe().f_code.co_name)
                break
            else:
                print("----完成监控任务调度结果输入消息----", "line: ", str(sys._getframe().f_lineno),\
                      "in ", sys._getframe().f_code.co_name)
                break

        else:
            print("----s_l_result_detail----输入没有任务数据！", "line: ", str(sys._getframe().f_lineno),\
                  "in ", sys._getframe().f_code.co_name)
            sleep(10)
            n += 1
            if n == 10:
                break

# 监控任务执行结果输出
def check_s_l_result_output(data):
    print("----开始监控任务调度结果输出消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
    result_output_select_result = ms.ExecuQuery(data.encode('utf-8'))
    n = 0
    while 1:
        if result_output_select_result:
            if result_output_select_result[0]["status"] == 1:
                print("----s_l_result_detail----输出任务执行中！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], \
                         result_output_select_result[0]["slice_type"], str(result_output_select_result[0]["slice_time"]), \
                         str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"], \
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], \
                         result_output_select_result[0]["dataformat_name"]))
                sleep(5)
                continue
            elif result_output_select_result[0]["status"] == 2:
                print("----s_l_result_output----输出任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], \
                         result_output_select_result[0]["slice_type"], str(result_output_select_result[0]["slice_time"]), \
                         str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"], \
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], \
                         result_output_select_result[0]["dataformat_name"]))
                print("----完成监控任务调度结果输出消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            elif result_output_select_result[0]["status"] == 3:
                print("----s_l_result_output----输出任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], \
                         result_output_select_result[0]["slice_type"], str(result_output_select_result[0]["slice_time"]), \
                         str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"], \
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], \
                         result_output_select_result[0]["dataformat_name"]))
                print("----完成监控任务调度结果输出消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            elif result_output_select_result[0]["status"] == -2:
                print("----s_l_result_output----输出任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], \
                         result_output_select_result[0]["slice_type"], str(result_output_select_result[0]["slice_time"]), \
                         str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"], \
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], \
                         result_output_select_result[0]["dataformat_name"]))
                print("----完成监控任务调度结果输出消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            elif result_output_select_result[0]["status"] == -3:
                print("----s_l_result_output----输出任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s" \
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], \
                         result_output_select_result[0]["slice_type"], str(result_output_select_result[0]["slice_time"]), \
                         str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"], \
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], \
                         result_output_select_result[0]["dataformat_name"]))
                print("----完成监控任务调度结果输出消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break
            else:
                print("----完成监控任务调度结果输出消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
                break

        else:
            result_output_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("----s_l_result_output----输出没有任务数据！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name)
            sleep(10)
            n += 1
            if n == 10:
                break

s_l_message="select full_name,status,slice_type ,slice_time,create_time,row_number from s_l_message order by create_time desc limit 1"
s_r_task = "select task_oid,job_oid,status,slice_type ,slice_time ,create_time,row_number,cdo_name, dataformat_name from s_r_task order by create_time desc limit 1"
s_l_result_task = "select job_oid,status,slice_type ,slice_time ,create_time,row_number_output,task_name, result_oid from s_l_result_task order by create_time desc limit 1"
s_l_result_detail = "select job_oid,status,slice_type ,slice_time ,create_time,row_number_init,task_name, cdo_name ,dataformat_name from s_l_result_detail order by create_time desc limit 1"
s_l_result_output = "select job_oid,status,slice_type ,slice_time ,create_time,row_number_init,task_name, cdo_name ,dataformat_name from s_l_result_output order by create_time desc limit 1"
cd = "cd /data/input/demo/"
sh = "sh createdata2.sh"
mv = "mv [demo]* gdemo/"

def run_all():
    log.debug("开始执行脚本")
    host.connect()
    host.send(cd)  # 发送一个命令
    host.send('sh createdata2.sh')
    sleep(7)
    host.send(sh)
    sleep(3)
    host.send(mv)
    check_s_l_message(s_l_message)
    check_s_r_task(s_r_task)
    check_s_l_result_task(s_l_result_task)
    check_s_l_result_detail(s_l_result_detail)
    check_s_l_result_output(s_l_result_output)
    log.debug("完成脚本执行")
run_all()