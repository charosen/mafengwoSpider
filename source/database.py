#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 模块字符串：
'''
Defines a Data Saver classes allow users to save all resorts infos data fetched
from different website.
'''


# 导入模块：
import json
import os
import pymysql

from pymongo import MongoClient
from py2neo import Node, Relationship, Graph
from settings import NEO_CONF, MONGO_CONF, SQL_CONF, \
                     save_path, table_name, collection


# 全局变量：
RESORT_SQL = '''CREATE TABLE IF NOT EXISTS {0}(
    poi_id INTEGER NOT NULL,
    resortName VARCHAR(60),
    areaName   VARCHAR(30),
    areaId     INTEGER NOT NULL,
    address    VARCHAR(128),
    lat        FLOAT,
    lng        FLOAT,
    introduction TEXT,
    openInfo   VARCHAR(255),
    ticketsInfo VARCHAR(512),
    transInfo  TEXT,
    tel        VARCHAR(128),
    item_site  VARCHAR(128),
    item_time  VARCHAR(128),
    payAbstracts TEXT,
    source     VARCHAR(30),
    timeStamp  VARCHAR(30)
    );'''


# 类定义：

# 数据存储器基类
class BaseSaver(object):
    # 文档字符串
    '''
    BaseSaver class allows users to save all infos data fetched from website.

    :Usage:

    '''
    # 数据存储器的静态成员定义
    SAVE_MODES = ('mongodb', 'neo4j', 'mysql')
    # 初始化方法：
    def __init__(self, save_mode="neo4j"):
        # 文档字符串
        '''
        Initialize an instance of BaseSaver.

        :Args:
         - save_mode : a str of database to save data in.

        '''
        # 方法实现
        if save_mode not in self.SAVE_MODES:
            raise RuntimeError('存储模式指定有误，请输入mongodb、neo4j或者mysql')
        self.save_mode = save_mode
        if self.save_mode == 'mongodb':
            # mongodb initialize
            print('>>>> we are in mongodb.')
            self.connector = MongoClient(**MONGO_CONF)[MONGO_CONF.get('authSource')]
        elif self.save_mode == 'neo4j':
            # neo4j initialize
            print('>>>> we are in neo4j.')
            self.connector = Graph(**NEO_CONF)
        else:
            # mysql initialize
            print('>>>> we are in mysql.')
            self.connector = pymysql.connect(**SQL_CONF)
            self.cursor = self.connector.cursor()
            sql = RESORT_SQL.format(table_name)
            print(sql)
            self.cursor.execute(sql)
            self.connector.commit()


    # 数据存储方法：
    def data_save(self, file_name):
        # 文档字符串
        '''
        Saves spider fetched data into different databases.
        Wipes out the old data and saves the new fetched ones.

        :Args:
         - file_name : a str of file name to fetch data from.

        '''
        # 方法实现
        # 此处可以拓展成任意文件类型，其他文件类型的数据转换成json再写即可
        file_path = os.path.join(save_path, file_name+'.json')
        if not os.access(file_path, os.F_OK):
            raise RuntimeError(f'数据文件{file_path}不存在，请检查数据！')
        with open(file_path, 'r', encoding='utf-8') as file:
            self.json_data = json.load(file, encoding='utf-8')

        if self.save_mode == 'mongodb':
            print('>>> we are saving to mongodb.')
            # 删除原始数据
            self.connector.drop_collection(collection)
            # 保存新数据
            self.connector[collection].insert_many(self.json_data)
        elif self.save_mode == 'neo4j':
            print('>>> we are saving to neo4j.')
            # 删除原始数据, 一定要小心使用
            self.graph_cleaner()
            # 保存新数据
            self.graph_builder()
        else:
            print('>>> we are saving to mysql.')
            # 删除原始数据，一定要小心使用
            self.cursor.execute(f"DELETE FROM {table_name}")
            # 准备sql语句
            data_key = self.json_data[0].keys()
            sql_key = ','.join(data_key)
            sql_value = ', '.join([f'%({key})s' for key in data_key])
            # 保存新数据
            sql = '''
            INSERT INTO {0}({1})
            VALUES ({2});
            '''.format(table_name, sql_key, sql_value)
            print(sql)
            self.cursor.executemany(sql, self.json_data)
            self.connector.commit()


    # 知识图谱删除方法：
    def graph_cleaner(self):
        pass


    # 知识图谱生成方法：
    def graph_builder(self):
        pass


    # 数据存储器退出方法：
    def __del__(self):
        # 文档字符串
        '''
        The deconstructor of BaseSaver class.

        Deconstructs an instance of BaseSaver, closes Databases.
        '''
        # 方法实现
        print(f'>>>> closing {self.save_mode}.')
        if self.save_mode == 'mongodb':
            self.connector.client.close()
        elif self.save_mode == 'mysql':
            self.connector.close()


# 马蜂窝数据存储器子类：
class MafengwoSaver(BaseSaver):
    # 文档字符串
    '''
    Defines a MafengwoSaver class inherited from BaseSaver class.

    MafengwoSaver class allows users to save all resorts infos data fetched from
    mafengwo website.

    :Usage:

    '''
    # 数据存储器静态成员定义

    # 知识图谱删除方法
    def graph_cleaner(self):
        # 文档字符串
        '''
        Breaks down knowledge graph of mafengwo resorts data in Graph Database
        Neo4j.

        Detachs isLocateOf relationship, then deletes locate nodes and resort
        nodes.
        '''
        self.connector.run("match (n:locate)-[]->(m:resort) detach delete n, m")


    # 知识图谱生成方法
    def graph_builder(self):
        # 文档字符串
        '''
        Builds a knowledge graph of mafengwo resorts data in Graph Database Neo4j.

        Creates locate nodes and resort nodes, then creates isLocateOf relationship
        between them.
        '''
        # 方法实现
        for info in self.json_data:
            print('>> saving:', info)
            areaInfo = {
                'address': info['address'], 'areaId': info['areaId'],
                'areaName': info['areaName'], 'lat': info['lat'],
                'lng': info['lng'], 'source': info['source'],
                'timeStamp': info['timeStamp']
            }
            areaNode = Node("locate", **areaInfo)
            resortNode = Node("resort", **info)
            self.connector.create(areaNode | resortNode)
            self.connector.merge(Relationship(areaNode, 'isLocateOf', resortNode))




# class DataSaver(object):
#     # 文档字符串
#     '''
#     DataSaver class allows users to save all resorts infos data fetched from
#     mafengwo website.
#
#     :Usage:
#
#     '''
#     # 数据存储器静态成员定义
#     savemodes = ('mongodb', 'neo4j', 'mysql')
#     # 初始化方法：
#     def __init__(self, saveMode="neo4j"):
#         # 文档字符串
#         '''
#         Initialize an instance of DataSaver.
#
#         :Args:
#          - saveMode : a str of database to save data in.
#         '''
#         # 方法实现
#         if saveMode not in self.savemodes:
#             raise RuntimeError('存储模式指定有误，请输入mongodb、neo4j或者mysql')
#         self.saveMode = saveMode
#         if self.saveMode == 'mongodb':
#             # mongodb initialize
#             print('>>>> we are in mongodb.')
#             self.connector = MongoClient(**MONGO_CONF)[MONGO_CONF.get('authSource')]
#         elif self.saveMode == 'neo4j':
#             # neo4j initialize
#             print('>>>> we are in neo4j.')
#             self.connector = Graph(**NEO_CONF)
#         else:
#             # mysql initialize
#             print('>>>> we are in mysql.')
#             self.connector = pymysql.connect(**SQL_CONF)
#             self.cursor = self.connector.cursor()
#             sql = RESORT_SQL.format(table_name)
#             print(sql)
#             self.cursor.execute(sql)
#             self.connector.commit()
#
#
#     def dataSave(self):
#         # 文档字符串
#         '''
#         Saves spider resorts data into different database.
#
#         Wipes out the old data and saves the new fetched ones.
#         '''
#         # 方法实现
#         file_path = os.path.join(save_path,file_name+'.json')
#         if not os.access(file_path, os.F_OK):
#             raise RuntimeError('景点数据文件不存在，请检查数据！')
#         with open(file_path, 'r', encoding='utf-8') as file:
#             self.json_data = json.load(file, encoding='utf-8')
#
#         if self.saveMode == 'mongodb':
#             print('>>> we are saving to mongodb.')
#             # 删除原始数据
#             self.connector.drop_collection(collection)
#             # 保存新数据
#             self.connector[collection].insert_many(self.json_data)
#         elif self.saveMode == 'neo4j':
#             print('>>> we are saving to neo4j.')
#             # 删除原始数据, 一定要小心使用
#             self.graph_cleaner()
#             # 保存新数据
#             self.graphBuilder()
#         else:
#             print('>>> we are saving to mysql.')
#             # 删除原始数据，一定要小心使用
#             self.cursor.execute(f"DELETE FROM {table_name}")
#             # 保存新数据
#             sql = '''
#             INSERT INTO {0}(poi_id,resortName,areaName,areaId,address,lat,lng,
#                     introduction,openInfo,ticketsInfo,transInfo,tel,item_site,
#                     item_time,payAbstracts,source,timeStamp)
#             VALUES (%(poi_id)s, %(resortName)s, %(areaName)s, %(areaId)s, %(address)s,
#                     %(lat)s, %(lng)s, %(introduction)s, %(openInfo)s, %(ticketsInfo)s,
#                     %(transInfo)s, %(tel)s, %(item-site)s, %(item-time)s, %(payAbstracts)s,
#                     %(source)s, %(timeStamp)s);
#             '''.format(table_name)
#             print(sql)
#             self.cursor.executemany(sql, self.json_data)
#             self.connector.commit()
#
#
#     def graph_cleaner(self):
#         self.connector.run("match (n:locate)-[]->(m:resort) detach delete n, m")
#
#
#     def graphBuilder(self):
#         # 文档字符串
#         '''
#         Builds a knowledge graph of mafengwo resorts data in Graph Database Neo4j.
#
#         Creates locate nodes and resort nodes, then creates isLocateOf relationship
#         between them.
#         '''
#         # 方法实现
#         for info in self.json_data:
#             print('>> saving:', info)
#             areaInfo = {
#                 'address': info['address'], 'areaId': info['areaId'],
#                 'areaName': info['areaName'], 'lat': info['lat'],
#                 'lng': info['lng'], 'source': info['source'],
#                 'timeStamp': info['timeStamp']
#             }
#             areaNode = Node("locate", **areaInfo)
#             resortNode = Node("resort", **info)
#             self.connector.create(areaNode | resortNode)
#             self.connector.merge(Relationship(areaNode, 'isLocateOf', resortNode))






# 测试代码：
if __name__ == '__main__':
    saver = MafengwoSaver('mysql')
    saver.data_save('HainanResorts')
