from util.timestamp_13 import get_now_time
from util.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG


class CleanData(object):
    def __init__(self):
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"],MySQL_CONFIG["PORT"])

    def clean_datasource_test_data(self):
        dss_sql = "delete from merce_dss where creator = 'admin'  and  name not like '%测试用%' "
        # print(dss_sql)
        try:
            self.ms.ExecuNoQuery(dss_sql)
        except TypeError as e:
            print('66666')

    def clean_dataset_test_data(self):
        dt_sql = "delete from merce_dataset where creator = 'admin'  and  name not like '%测试用%' and name like 'API_datasets%' "
        try:
            self.ms.ExecuNoQuery(dt_sql)
        except TypeError as e:
            print('66666')

# CleanData().clean_dataset_test_data()

