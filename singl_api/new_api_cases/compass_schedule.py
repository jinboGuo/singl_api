from time import sleep
import sys
from basic_info.setting import Compass_MySQL_CONFIG, Compass_scheduler
from util.Open_DB import MYSQL
from util.conn_linux import Linux

ms = MYSQL(Compass_MySQL_CONFIG["HOST"], Compass_MySQL_CONFIG["USER"], Compass_MySQL_CONFIG["PASSWORD"], Compass_MySQL_CONFIG["DB"])
host = Linux(Compass_scheduler["HOST"], Compass_scheduler["USER"], Compass_scheduler["PASSWORD"])

# 监控kafka消息
def check_s_l_message(data):
    print("\033[35m----开始监控发送kafka消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
    message_select_result = ms.ExecuQuery(data.encode('utf-8'))
    while 1:
        if message_select_result:
            print("\033[35m----s_l_message----存在kafka消息数据！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
            print("job_data_oid: %s full_name: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number: %s"\
              % (str(message_select_result[0]["job_data_oid"]), message_select_result[0]["full_name"], message_select_result[0]["status"], message_select_result[0]["slice_type"],\
                 str(message_select_result[0]["slice_time"]),str(message_select_result[0]["create_time"]), str(message_select_result[0]["row_number"])))
            sleep(5)
            message_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if message_select_result:
                sleep(5)
                continue
            else:
                print("\033[35m----s_l_message----kafka消息数据被删除！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("\033[35m----完成监控发送kafka消息----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
        else:
            message_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("\033[31m----s_l_message----没有任务数据！", "line: ", str(sys._getframe().f_lineno),"in ", sys._getframe().f_code.co_name, "\033[0m")
            sleep(5)

# 监控日志调度任务
def check_s_r_task(data):
    print("\033[35m----开始监控日志调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
    task_select_result = ms.ExecuQuery(data.encode('utf-8'))
    while 1:
        if task_select_result:
            if task_select_result[0]["status"] == 0:
                print("\033[35m----s_r_task----日志调度任务待执行！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
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
                    print("\033[35m----s_r_task----日志调度任务执行中！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                    print("task_oid: %s job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number: %s dataset_name: %s dataformat_name: %s"\
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
                                print("\033[35m----s_r_task----日志调度任务数据被删除！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                                print("\033[35m----完成监控日志调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                                break
                    except:
                        return
            elif task_select_result[0]["status"] == 1:
                print("\033[35m----s_r_task----日志调度任务执行中！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("task_oid: %s job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number: %s dataset_name: %s dataformat_name: %s"\
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
                            print("\033[35m----s_r_task----日志调度任务数据被删除！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                            print("\033[35m----完成监控日志调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                            break
                except:
                    return
            else:
                if len(task_select_result) == 0:
                    print("\033[35m----s_r_task----日志调度任务数据被删除！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                    print("\033[35m----完成监控日志调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                    break

        else:
            task_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("\033[31m----s_r_task----没有日志调度任务数据！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
            sleep(5)

# 监控调度任务执行
def check_s_l_result_task(data):
    print("\033[35m----开始监控调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
    result_task_select_result = ms.ExecuQuery(data.encode('utf-8'))
    while 1:
        if result_task_select_result:
            if result_task_select_result[0]["status"] == 1:
                print("\033[35m----s_l_result_task----调度任务执行中！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s"\
                      % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"], result_task_select_result[0]["slice_type"],\
                         str(result_task_select_result[0]["slice_time"]),str(result_task_select_result[0]["create_time"]),\
                         result_task_select_result[0]["row_number_output"], result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                result_task_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if result_task_select_result:
                    if result_task_select_result[0]["status"] == 1:
                        sleep(5)
                        continue
                    elif result_task_select_result[0]["status"] == 2:
                        print("\033[35m----s_l_result_task----调度任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                        print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s"\
                              % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"], result_task_select_result[0]["slice_type"],\
                                 str(result_task_select_result[0]["slice_time"]),str(result_task_select_result[0]["create_time"]),\
                                 result_task_select_result[0]["row_number_output"], result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                        print("\033[35m----完成监控调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                        break
                    elif result_task_select_result[0]["status"] == -2:
                        print("\033[31m----s_l_result_task----调度任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                        print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s"\
                              % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"], result_task_select_result[0]["slice_type"],\
                                 str(result_task_select_result[0]["slice_time"]),str(result_task_select_result[0]["create_time"]),\
                                 result_task_select_result[0]["row_number_output"], result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                        print("\033[35m----完成监控调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                        break
                else:
                    return
            elif result_task_select_result[0]["status"] == 2:
                print("\033[35m----s_l_result_task----调度任务执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s"\
                      % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"], result_task_select_result[0]["slice_type"],\
                         str(result_task_select_result[0]["slice_time"]),str(result_task_select_result[0]["create_time"]),\
                         result_task_select_result[0]["row_number_output"], result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                print("\033[35m----完成监控调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            elif result_task_select_result[0]["status"] == -2:
                print("\033[31m----s_l_result_task----调度任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_output: %s task_name: %s result_oid: %s"\
                      % (str(result_task_select_result[0]["job_oid"]), result_task_select_result[0]["status"], result_task_select_result[0]["slice_type"],\
                         str(result_task_select_result[0]["slice_time"]), str(result_task_select_result[0]["create_time"]),\
                         result_task_select_result[0]["row_number_output"], result_task_select_result[0]["task_name"], result_task_select_result[0]["result_oid"]))
                print("\033[35m----完成监控调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            else:
                print("\033[31m----s_l_result_task----调度任务执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("\033[35m----完成监控调度任务数据----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break

        else:
            result_task_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("\033[31m----s_l_result_task----没有调度任务数据！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
            sleep(5)

# 监控调度任务执行输入结果
def check_s_l_result_detail(data):
    print("\033[35m----开始监控调度任务输入结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
    result_detail_select_result = ms.ExecuQuery(data.encode('utf-8'))
    n = 0
    while 1:
        if result_detail_select_result:
            if result_detail_select_result[0]["status"] == 2:
                print("\033[35m----s_l_result_detail----调度任务输入结果执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], result_detail_select_result[0]["slice_type"],\
                         str(result_detail_select_result[0]["slice_time"]),str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"],\
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], result_detail_select_result[0]["dataformat_name"]))
                print("\033[35m----完成监控调度任务输入结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            elif result_detail_select_result[0]["status"] == 3:
                print("\033[35m----s_l_result_detail----调度任务输入结果执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], result_detail_select_result[0]["slice_type"],\
                         str(result_detail_select_result[0]["slice_time"]),str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"],\
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], result_detail_select_result[0]["dataformat_name"]))
                print("\033[35m----完成监控调度任务输入结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            elif result_detail_select_result[0]["status"] == -2:
                print("\033[31m----s_l_result_detail----调度任务输入结果执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], result_detail_select_result[0]["slice_type"],\
                         str(result_detail_select_result[0]["slice_time"]),str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"],\
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], result_detail_select_result[0]["dataformat_name"]))
                print("\033[35m----完成监控调度任务输入结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            elif result_detail_select_result[0]["status"] == -3:
                print("\033[31m----s_l_result_detail----调度任务输入结果执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_detail_select_result[0]["job_oid"]), result_detail_select_result[0]["status"], result_detail_select_result[0]["slice_type"],\
                         str(result_detail_select_result[0]["slice_time"]),str(result_detail_select_result[0]["create_time"]), result_detail_select_result[0]["row_number_init"],\
                         result_detail_select_result[0]["task_name"], result_detail_select_result[0]["cdo_name"], result_detail_select_result[0]["dataformat_name"]))
                print("\033[35m----完成监控调度任务输入结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            else:
                print("\033[35m----完成监控调度任务输入结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break

        else:
            print("\033[31m----s_l_result_detail----调度任务输入结果执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
            print("\033[31m----s_l_result_detail----没有调度任务输入结果！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
            sleep(10)
            n += 1
            if n == 5:
                break

# 监控调度任务执行输出结果
def check_s_l_result_output(data):
    print("\033[35m----开始监控调度任务输出结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
    result_output_select_result = ms.ExecuQuery(data.encode('utf-8'))
    n = 0
    while 1:
        if result_output_select_result:
            if result_output_select_result[0]["status"] == 2:
                print("\033[35m----s_l_result_output----调度任务输出结果执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], result_output_select_result[0]["slice_type"],\
                         str(result_output_select_result[0]["slice_time"]), str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"],\
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], result_output_select_result[0]["dataformat_name"]))
                print("\033[35m----完成监控调度任务输出结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            elif result_output_select_result[0]["status"] == 3:
                print("\033[35m----s_l_result_output----调度任务输出结果执行成功！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], result_output_select_result[0]["slice_type"],\
                         str(result_output_select_result[0]["slice_time"]), str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"],\
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], result_output_select_result[0]["dataformat_name"]))
                print("\033[35m----完成监控调度任务输出结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            elif result_output_select_result[0]["status"] == -2:
                print("\033[31m----s_l_result_output----调度任务输出结果执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], result_output_select_result[0]["slice_type"],\
                         str(result_output_select_result[0]["slice_time"]), str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"],\
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], result_output_select_result[0]["dataformat_name"]))
                print("\033[35m----完成监控调度任务输出结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            elif result_output_select_result[0]["status"] == -3:
                print("\033[31m----s_l_result_output----调度任务输出结果执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("job_oid: %s status: %s slice_type: %s slice_time: %s create_time: %s row_number_init: %s task_name: %s cdo_name: %s dataformat_name: %s"\
                      % (str(result_output_select_result[0]["job_oid"]), result_output_select_result[0]["status"], result_output_select_result[0]["slice_type"],\
                         str(result_output_select_result[0]["slice_time"]), str(result_output_select_result[0]["create_time"]), result_output_select_result[0]["row_number_init"],\
                         result_output_select_result[0]["task_name"], result_output_select_result[0]["cdo_name"], result_output_select_result[0]["dataformat_name"]))
                print("\033[35m----完成监控调度任务输出结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break
            else:
                print("\033[31m----s_l_result_output----调度任务输出结果执行失败！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                print("\033[35m----完成监控调度任务输出结果----", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
                break

        else:
            result_output_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("\033[31m----s_l_result_output----没有调度任务输出结果！", "line: ", str(sys._getframe().f_lineno), "in ", sys._getframe().f_code.co_name, "\033[0m")
            sleep(10)
            n += 1
            if n == 5:
                break

s_l_message="select job_data_oid,full_name,status,slice_type ,slice_time,create_time,row_number from s_l_message order by create_time desc limit 1"
s_r_task = "select task_oid,job_oid,status,slice_type ,slice_time ,create_time,row_number,cdo_name, dataformat_name from s_r_task order by create_time desc limit 1"
s_l_result_task = "select job_oid,status,slice_type ,slice_time ,create_time,row_number_output,task_name, result_oid from s_l_result_task order by create_time desc limit 1"
s_l_result_detail = "select job_oid,status,slice_type ,slice_time ,create_time,row_number_init,task_name, cdo_name ,dataformat_name from s_l_result_detail order by create_time desc limit 1"
s_l_result_output = "select job_oid,status,slice_type ,slice_time ,create_time,row_number_init,task_name, cdo_name ,dataformat_name from s_l_result_output order by create_time desc limit 1"
cd = "cd /data/input/demo/"
sh = "sh createdata2.sh"
mv = "mv [demo]* gdemo/"

def run_all():
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
run_all()