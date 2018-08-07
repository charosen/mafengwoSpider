#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# 模块字符串：
'''
Defines a DataSaver class allows users to save all resorts infos data fetched
from mafengwo website.
'''


# 导入模块：
import json
import os
import pymysql

from pymongo import MongoClient
from py2neo import Node, Relationship, Graph


# 全局变量：
# 数据存储路径和文件名（.csv or .txt）配置变量：
savePath = "./mafengwoResortsInfos"
filename = "HainanResorts"
# Neo4j数据库配置：
NeoConf = {
    "host": "localhost", "port": 7687,
    "user": "neo4j", "password": "Crz437991"
}
# MongoDB数据库配置：
collection = "HainanResorts"
MongoConf = {
    "host": "localhost", "port": 27017, "database": "test",
    "user": "test", "password": "Crz437991"
}
# MySQL数据库配置：
tableName = "HainanResorts"
SqlConf = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "test",
    "passwd": "Crz437991",
    "database": "test",
    "charset": "utf8"
}

# 类定义：
class DataSaver(object):
    # 文档字符串
    '''
    DataSaver class allows users to save all resorts infos data fetched from
    mafengwo website.

    :Usage:

    '''
    # 数据存储器静态成员定义
    savemodes = ('mongodb', 'neo4j', 'mysql')
    # 初始化方法：
    def __init__(self, saveMode="neo4j"):
        # 文档字符串
        '''
        Initialize an instance of DataSaver.

        :Args:
         - saveMode : a str of database to save data in.
        '''
        # 方法实现
        if saveMode not in self.savemodes:
            raise RuntimeError('存储模式指定有误，请输入mongodb、neo4j或者mysql')
        self.saveMode = saveMode
        if self.saveMode == 'mongodb':
            # mongodb initialize
            print('>>>> we are in mongodb.')
            self.client = MongoClient(MongoConf['host'], MongoConf['port'])
            self.connector = self.client[MongoConf['database']]
            self.connector.authenticate(MongoConf['user'], MongoConf['password'])
        elif self.saveMode == 'neo4j':
            # neo4j initialize
            print('>>>> we are in neo4j.')
            self.connector = Graph(**NeoConf)
        else:
            # mysql initialize
            print('>>>> we are in mysql.')
            self.connector = pymysql.connect(**SqlConf)
            self.cursor = self.connector.cursor()
            sql = '''CREATE TABLE IF NOT EXISTS {0}(
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
                );'''.format(tableName)
            print(sql)
            self.cursor.execute(sql)
            self.connector.commit()


    def dataSave(self):
        # 文档字符串
        '''
        Saves spider resorts data into different database.

        Wipes out the old data and saves the new fetched ones.
        '''
        # 方法实现
        filePath = os.path.join(savePath,filename+'.json')
        if not os.access(filePath, os.F_OK):
            raise RuntimeError('景点数据文件不存在，请检查数据！')
        with open(filePath, 'r', encoding='utf-8') as file:
            self.resortInfos = json.load(file, encoding='utf-8')

        if self.saveMode == 'mongodb':
            print('>>> we are saving to mongodb.')
            # 删除原始数据
            self.connector.drop_collection('HainanResorts')
            # 保存新数据
            self.connector.HainanResorts.insert_many(self.resortInfos)
        elif self.saveMode == 'neo4j':
            print('>>> we are saving to neo4j.')
            # 删除原始数据, 一定要小心使用
            self.connector.run("match (n:locate)-[]->(m:resort) detach delete n, m")
            # 保存新数据
            self.graphBuilder()
        else:
            print('>>> we are saving to mysql.')
            # 删除原始数据，一定要小心使用
            self.cursor.execute(f"DELETE FROM {tableName}")
            # 保存新数据
            sql = '''
            INSERT INTO {0}(poi_id,resortName,areaName,areaId,address,lat,lng,
                    introduction,openInfo,ticketsInfo,transInfo,tel,item_site,
                    item_time,payAbstracts,source,timeStamp)
            VALUES (%(poi_id)s, %(resortName)s, %(areaName)s, %(areaId)s, %(address)s,
                    %(lat)s, %(lng)s, %(introduction)s, %(openInfo)s, %(ticketsInfo)s,
                    %(transInfo)s, %(tel)s, %(item-site)s, %(item-time)s, %(payAbstracts)s,
                    %(source)s, %(timeStamp)s);
            '''.format(tableName)
            print(sql)
            self.cursor.executemany(sql, self.resortInfos)
            self.connector.commit()

    def graphBuilder(self):
        # 文档字符串
        '''
        Builds a knowledge graph of mafengwo resorts data in Graph Database Neo4j.

        Creates locate nodes and resort nodes, then creates isLocateOf relationship
        between them.
        '''
        # 方法实现
        for info in self.resortInfos:
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


    def __del__(self):
        # 文档字符串
        '''
        The deconstructor of DataSaver class.

        Deconstructs an instance of DataSaver, closes Databases.
        '''
        # 方法实现
        print(f'>>>> closing {self.saveMode}.')
        if self.saveMode == 'mongodb':
            self.client.close()
        elif self.saveMode == 'mysql':
            self.connector.close()

# 测试代码：
if __name__ == '__main__':
    saver = DataSaver('neo4j')
    saver.dataSave()
