import time

import delta as delta


def timestamp_to_13(digits=13):
    # time_stamp = data_from_db.schema()
    time_stamp1 = time.strftime('%a %b %d %H:%M:%S %Y')
    # print(time_stamp1)
    time_stamp2 = time.mktime(time.strptime(time_stamp1))
    # print(time_stamp2)
    digits = 10 ** (digits - 10)
    time_stamp = int(round(time_stamp2*digits))
    return time_stamp

# print(timestamp_to_13(digits=13))


def get_timestamp(n):
    return str(int(time.time())*1000 - n*24*60*60*1000)


def get_now_time():
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%d')
    # now_time_date = datetime.datetime.strptime(now_time_nyr, '%Y-%m-%d')
    # print(now_time_nyr)
    yesterday_time = now_time + datetime.timedelta(days=-1)
    yesterday_time_nyr = yesterday_time.strftime('%Y-%m-%d')
    # yesterday_time_date = datetime.datetime.strptime(yesterday_time_nyr, '%Y-%m-%d')
    # return yesterday_time_date, now_time_date
    return yesterday_time_nyr, now_time_nyr

def get_now():
    """
    :return: 获取精确毫秒时间戳,13位
    """
    millis = int(round(time.time() * 1000))
    #print("now:", millis)
    return millis

def get_tomorrow():
    import datetime,time
    # 生成13时间戳   eg:1557842280000
    now = datetime.datetime.now()
    tomorrow = now + datetime.timedelta(hours=1)
    #print(tomorrow.strftime("%Y-%m-%d %H:%M:%S"))
    #print(tomorrow)
    # 10位，时间点相当于从1.1开始的当年时间编号
    date_stamp = str(int(time.mktime(tomorrow.timetuple())))
    # 3位，微秒
    data_microsecond = str("%06d" % tomorrow.microsecond)[0:3]
    date_stamp = date_stamp + data_microsecond
    #print("tomorrow:", date_stamp)
    return int(date_stamp)

from datetime import datetime, timedelta

def timestamp_now():
    now_time = datetime.now().isoformat()
    #print("ISO格式的日期和时间是 %s" % now_time)
    time = datetime.strptime(now_time, "%Y-%m-%dT%H:%M:%S.%f")
    times = str(time)[:-3]
    #print(times)
    return times
def timestamp_utc():
    now_time = datetime.now().isoformat()
    #print("ISO格式的日期和时间是 %s" % now_time)
    #time = datetime.strptime(now_time, "%Y-%m-%dT%H:%M:%S.%f")
    times = str(now_time)[:-3]+"Z"
    #print(times)
    return times
#timestamp_now1()

def datatime_now():
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%dT%H:%M:%S')
    time =str(now_time_nyr) + '.000+0000'
    return str(time)

def data_now():
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%d %H:%M:%S')
    #print(now_time_nyr)
    return str(now_time_nyr)

def hour_slice():
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%d %H')+":00:00"
    #print(now_time_nyr)
    return str(now_time_nyr)

def hour_stamp():
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y%m%d%H')
    #print(now_time_nyr)
    return str(now_time_nyr)
#get_tomorrow()