# coding:utf-8

"""
数据库操作的封装
"""

import pymysql
from configs import *


class MysqlDB():

    def start_db(self):
        '''
         与数据库建立连接
         '''
        self.db = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT,
            database=MYSQL_DARABASE,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def close(self):
        '''
         关闭数据库连接
         '''
        self.db.close()

    def save_one_data(self,table, data):
        '''
         将一条记录保存到数据库
         Args:
             table: 表名字 str
             data:  记录 dict
         return:
             成功： dict 保存的记录
             失败： -1
         每条记录都以一个字典的形式传进来
         '''
        self.start_db()
        if len(data) == 0:
            return -1
        fields = ''
        values = ''
        datas = {}
        for k, v in data.items():
            # 防止sql注入
            datas.update({k: pymysql.escape_string(v)})
        for d in datas:
            fields += "`{}`,".format(str(d)) # 反引号 避免关键字冲突
            values += "'{}',".format(str(data[d]))
        if len(fields) <= 0 or len(values) <= 0:
            return -1
        # 生成sql语句
        sql = "insert ignore into {}({}) values ({})".format(
            table, fields[:-1], values[:-1])
        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql)
                self.db.commit()
                cursor.close()
        except Exception as e:
            print(e)
            self.db.rollback()
        finally:
            self.close()

    def find_all(self, table, limit=-1):

        try:
            self.start_db()
            with self.db.cursor() as cursor:
                if limit == -1:
                    sql = "select * from {}".format(table)
                else:
                    sql = "select * from {} limit 0,{}".format(table, limit)
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except:
            print('数据库查询错误')
            return -1
        finally:
            self.close()


    def find_by_field(self, table, field, field_value):

        try:
            self.start_db()
            with self.db.cursor() as cursor:
                sql = "select * from {} where {} = '{}'".format(
                    table, field, field_value
                )
                cursor.execute(sql)
                result = cursor.fetchall()
                return  result
        except:
            print('数据库查询错误')
            return -1
        finally:
            self.close()

    def find_by_fields(self, table, queryset={}):

        try:
            self.start_db()
            with self.db.cursor() as cursor:
                querys = ""
                for k, v in queryset.items():
                    querys += "{} = '{}' and ".format(k, v)
                sql = "select * from {} where {} ".format(
                    table, querys[:-4])
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except:
            print('数据查询错误')
            return -1
        finally:
            self.close()

    def find_by_sort(self, table, field, limit=1000, order='DESC'):
        '''
        从数据库里查询排序过的数据
        :param table: 表名字
        :param field: 字段名
        :param limit: 限制数量
        :param order: 降序DESC/升序ASC 默认为降序
        :return: [dict] 保存的记录
        '''

        try:
            self.start_db()
            with self.db.cursor() as cursor:
                sql = "select * from {} order by {} {} limit 0,{}".format(
                    table, field, order, limit)
                cursor.execute(sql)
                result = cursor.fetchall
                return result
        except:
            print('数据库查询错误')
            return -1
        finally:
            self.close()

