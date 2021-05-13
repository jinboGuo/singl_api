# coding:utf-8
import pymysql


# 数据库连接
class MYSQL:
    def __init__(self, host, user, pwd, db,port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.port = port

    def _Getconnect(self):
        if not self.db:
            raise(NameError, '没有设置数据库连接信息')
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db,port= self.port,charset='utf8',
                                    cursorclass=pymysql.cursors.DictCursor)
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, '数据库连接失败')
        else:
            return cur

        # 执行查询
    def ExecuQuery(self, query):
        cur = self._Getconnect()
        cur.execute(query)
        # 获取查询到的全部数据
        res = cur.fetchall()
        # 关闭数据库连接并返回查询结果
        self.conn.close()
        return res

    def ExecuNoQuery(self, sql):
        cur = self._Getconnect()
        cur.execute(sql)
        res = cur.fetchall()
        self.conn.commit()
        self.conn.close()
        return res


# if __name__ == '__main__':
#      # from basic_info.setting import MySQL_CONFIG
#      ms = MYSQL('192.168.1.149', 'root', 'root', 'k8s_149',30306)
#      sql = 'select id,enabled from merce_flow order  by create_time desc limit 2'
#      res = ms.ExecuQuery(sql)
#      print(res)



# update_sql = 'update merce_schema set name = "8899" where id = "360e9d2d-1f4d-4283-bf10-ec232d7ab4ec"'
# res_update = ms.ExecuNoQuery(update_sql)

