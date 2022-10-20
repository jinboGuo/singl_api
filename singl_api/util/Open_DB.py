# coding:utf-8
import pymysql


# 数据库连接
class MYSQL:
    def __init__(self, host, user, pwd, db, port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.port = port

    def _Getconnect(self):
        if not self.db:
            raise(NameError, '没有设置数据库连接信息')
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, port=self.port, charset='utf8',
                                    cursorclass=pymysql.cursors.DictCursor)
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, '数据库连接失败')
        else:
            return cur

        # 执行查询
    def ExecuQuery(self, query):
        try:
            cur = self._Getconnect()
            cur.execute(query)
            # 获取查询到的全部数据
            res = cur.fetchall()
        # 关闭数据库连接并返回查询结果
        finally:
            self.conn.close()
        return res

    # 增删改
    def ExecuNoQuery(self, sql):
        try:
            cur = self._Getconnect()
            cur.execute(sql)
            res = cur.fetchall()
            self.conn.commit()
            self.conn.close()
            return res
        except:
            self.conn.rollback()

    # 批量插入
    def ExecutManyInsert(self, sql, values):
        try:
            cur = self._Getconnect()
            cur.executemany(sql, values)
            res = cur.fetchall()
            self.conn.commit()
            self.conn.close()
            return res
        except:
            self.conn.rollback()