import time


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

print(get_now_time()[1])
