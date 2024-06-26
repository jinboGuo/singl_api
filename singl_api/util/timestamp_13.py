# coding:utf8
import time
import math

def timestamp_to_13(digits=13):
    time_stamp1 = time.strftime('%a %b %d %H:%M:%S %Y')
    time_stamp2 = time.mktime(time.strptime(time_stamp1))
    digits = 10 ** (digits - 10)
    time_stamp = int(round(time_stamp2*digits))
    return time_stamp



def get_timestamp(n):
    return str(int(time.time())*1000 - n*24*60*60*1000)


def get_now_time():
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%d')
    yesterday_time = now_time + datetime.timedelta(days=-1)
    yesterday_time_nyr = yesterday_time.strftime('%Y-%m-%d')
    return yesterday_time_nyr, now_time_nyr


def get_now():
    """
    :return: 获取精确毫秒时间戳,13位
    """
    millis = int(round(time.time() * 1000))
    return millis


def get_tomorrow():
    """
    :return: 获取tomorrow
    """
    import datetime,time
    now = datetime.datetime.now()
    tomorrow = now + datetime.timedelta(hours=1)
    date_stamp = str(int(time.mktime(tomorrow.timetuple())))
    data_microsecond = str("%06d" % tomorrow.microsecond)[0:3]
    date_stamp = date_stamp + data_microsecond
    return int(date_stamp)


from datetime import datetime
def timestamp_now():
    """
    :return: 获取%Y-%m-%dT%H:%M:%S.%f
    """
    now_time = datetime.now().isoformat()
    times = datetime.strptime(now_time, "%Y-%m-%dT%H:%M:%S.%f")
    times = str(times)[:-3]
    return times


def timestamp_utc():
    """
    :return: 获取utc
    """
    now_time = datetime.now().isoformat()
    times = str(now_time)[:-3]+"Z"
    return str(times)


def datatime_now():
    """
    :return: 获取%Y-%m-%dT%H:%M:%S.000+0000
    """
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%dT%H:%M:%S')
    times =str(now_time_nyr) + '.000+0000'
    return str(times)


def data_now():
    """
    :return: 获取%Y-%m-%d %H:%M:%S
    """
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%d %H:%M:%S')
    return str(now_time_nyr)


def hour_slice():
    """
    :return: 获取%Y-%m-%d %H:00:00
    """
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%d %H')+":00:00"
    return str(now_time_nyr)


def hour_stamp():
    """
    :return: 获取%Y%m%d%H
    """
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y%m%d%H')
    return str(now_time_nyr)


def min_stamp():
    """
    :return: 获取%Y%m%d%H%M
    """
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y%m%d%H%M')
    return now_time_nyr


def second_stamp():
    """
    :return: 获取%Y%m%d%H%M%S
    """
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y%m%d%H%M%S')
    return now_time_nyr


def day_now():
    """
    :return: 获取%Y-%m-%d
    """
    import datetime
    now_time = datetime.datetime.now()
    now_time_nyr = now_time.strftime('%Y-%m-%d')
    return str(now_time_nyr)


def change_time(all_time):
    """
    @alltime时间转换min、hour
    """
    day = 24 * 60 * 60
    hour = 60 * 60
    minute = 60
    if all_time < 60:
        return "%d秒" % math.ceil(all_time)
    elif all_time > day:
        days = divmod(all_time, day)
        return "%d天%s" % (int(days[0]), change_time(days[1]))
    elif all_time > hour:
        hours = divmod(all_time, hour)
        return '%d小时%s' % (int(hours[0]), change_time(hours[1]))
    else:
        minutes = divmod(all_time, minute)
        return "%d分钟%d秒" % (int(minutes[0]), math.ceil(minutes[1]))



