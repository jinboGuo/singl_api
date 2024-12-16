# coding:utf-8
import pymysql
import ksycopg2
import cx_Oracle
from util.logs import Logger

log = Logger().get_log()


class MYSQL:
    def __init__(self, host, user, pwd, db, port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.port = port

    def _get_connect(self):
        if not self.db:
            raise(NameError, '没有设置mysql数据库连接信息')
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, port=self.port, charset='utf8')
        log.info("成功连接到 mysql 数据库")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, 'mysql数据库连接失败')
        else:
            return cur

    def ExecuQuery(self, query):
        try:
            cur = self._get_connect()
            cur.execute(query)
            res = cur.fetchall()
        finally:
            self.conn.close()
        return res


    def ExecuNoQuery(self, sql):
        try:
            cur = self._get_connect()
            cur.execute(sql)
            res = cur.fetchall()
            self.conn.commit()
            self.conn.close()
            return res
        except Exception as e:
            log.error("执行sql语句错误：%s" % e)
            self.conn.rollback()


    def ExecutManyInsert(self, sql, values):
        try:
            cur = self._get_connect()
            cur.executemany(sql, values)
            res = cur.fetchall()
            self.conn.commit()
            self.conn.close()
            return res
        except Exception as e:
            log.error("执行sql语句错误：%s" % e)
            self.conn.rollback()


class kingbase:
    def __init__(self, host, user, pwd, db, port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.port = port

    def _get_connect(self):
        if not self.db:
            raise(NameError, '没有设置数据库连接信息')
        self.conn = ksycopg2.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, port=self.port)
        log.info("成功连接到 king base 数据库")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, '数据库连接失败')
        else:
            return cur

    def ExecuQuery(self, query):
        try:
            cur = self._get_connect()
            cur.execute(query)
            res = cur.fetchall()
        finally:
            self.conn.close()
        return res

    def ExecuNoQuery(self, sql):
        try:
            cur = self._get_connect()
            cur.execute(sql)
            res = cur.fetchall()
            self.conn.commit()
            self.conn.close()
            return res
        except Exception as e:
            log.error("执行sql语句错误：%s" % e)
            self.conn.rollback()

    def ExecutManyInsert(self, sql, values):
        try:
            cur = self._get_connect()
            cur.executemany(sql, values)
            res = cur.fetchall()
            self.conn.commit()
            self.conn.close()
            return res
        except Exception as e:
            log.error("执行sql语句错误：%s" % e)
            self.conn.rollback()


class oracle:
    def __init__(self, host, user, pwd, db, port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.port = port

    def _get_connect(self):
        if not self.db:
            raise(NameError, '没有设置数据库连接信息')
        self.conn = cx_Oracle.connect(user=self.user, password=self.pwd,dsn=self.host+':'+str(self.port)+'/'+self.db)
        log.info("成功连接到 Oracle 数据库")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, '数据库连接失败')
        else:
            return cur


    def ExecuQuery(self, query):
        try:
            cur = self._get_connect()
            cur.execute(query)
            res = cur.fetchall()
        finally:
            self.conn.close()
        return res

    def ExecuNoQuery(self, sql):
        try:
            cur = self._get_connect()
            cur.execute(sql)
            res = cur.fetchall()
            self.conn.commit()
            self.conn.close()
            return res
        except Exception as e:
            log.error("执行sql语句错误：%s" % e)
            self.conn.rollback()


    def ExecutManyInsert(self, sql, values):
        try:
            cur = self._get_connect()
            cur.executemany(sql, values)
            res = cur.fetchall()
            self.conn.commit()
            self.conn.close()
            return res
        except Exception as e:
            log.error("执行sql语句错误：%s" % e)
            self.conn.rollback()