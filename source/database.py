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

from py2neo import Node, Relationship, Graph


# 全局变量：
# 数据存储路径和文件名（.csv or .txt）配置变量：
savePath = './mafengwoResortsInfos'
filename = 'HainanResorts'
# Neo4j数据库配置：
NeoConf = {
    "host": "182.92.86.34", "port": 7687,
    "user": "omada", "password": "bupt@2018"
}
# MongoDB数据库配置：
collection = "HainanResorts"
MongoConf = {
    "host": "localhost", "port": 27017, "database": "test",
    "user": "test", "password": "Crz437991"
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

        # 方法实现
        if saveMode not in self.savemodes:
            raise RuntimeError('存储模式指定有误，请输入mongodb、neo4j或者mysql')
        self.saveMode = saveMode
        if self.saveMode == 'mongodb':
            # mongodb initialize
            print('>>>> we are in mongodb.')

        elif self.saveMode == 'neo4j':
            # neo4j initialize
            print('>>>> we are in neo4j.')
            self.connector = Graph(**NeoConf)
        else:
            # mysql initialize
            print('>>>> we are in mysql.')


    def dataSave(self):
        # 文档字符串

        # 方法实现
        filePath = os.path.join(savePath,filename+'.json')
        if not os.access(filePath, os.F_OK):
            raise RuntimeError('景点数据文件不存在，请检查数据！')
        with open(filePath, 'r', encoding='utf-8') as file:
            self.resortInfos = json.load(file)

        if self.saveMode == 'mongodb':
            print('>>> we are saving to mongodb.')
        elif self.saveMode == 'neo4j':
            print('>>> we are saving to neo4j.')
            self.graphBuilder()
        else:
            print('>>> we are saving to mysql.')

    def graphBuilder(self):
        # 文档字符串

        # 方法实现
        for info in self.resortInfos:
            print(info)
            print(type(info))
            areaNode = Node("locate", address=info['address'],
                                      areaId=info['areaId'],
                                      areaName=info['areaName'],
                                      lat=info['lat'],
                                      lng=info['lng'],
                                      source=info['source'],
                                      timeStamp=info['timeStamp'])
            resortNode = Node("resort", **info)
            # tx.merge(areaNode)
            # tx.merge(resortNode)
            self.connector.create(areaNode)
            self.connector.create(resortNode)
            self.connector.merge(Relationship(areaNode, 'isLocateOf', resortNode))
            del areaNode
            del resortNode


    def __del__(self):
        # 文档字符串
        '''
        The deconstructor of DataSaver class.

        Deconstructs an instance of DataSaver, closes Databases.
        '''
        # 方法实现
        print(f'>>>> closing {self.saveMode}.')


# 测试代码：
if __name__ == '__main__':
    saver = DataSaver('neo4j')
    saver.dataSave()
