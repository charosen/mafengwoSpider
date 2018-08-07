#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
Define a MafengwoSpider class allows you to fetch mafengwo resorts infos in
given Province.
'''

import os
import re
import json
import time
import random

import requests
from lxml import etree


# 全局变量定义
# HTTP请求配置变量：
HEADERS = [
    {
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit'
                      '/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'),
    },
    {
        'User-Agent': ('Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 '
                      'Firefox/4.0.1')
    }
]

TIMEOUT = (6, 6)


# 数据存储路径和文件名（.csv or .txt）配置变量：
savePath = './mafengwoResortsInfos'
filename = 'HainanResorts'


class MafengwoSpider(object):
    # 文档字符串
    '''
    MafengwoSpider class allows users to fetch all data from mafengwo websites.

    :Usage:

    '''
    # 爬虫静态成员定义
    baseUrl = "http://www.mafengwo.cn/search/s.php?t=poi&kt=1"
    locationApi = "http://pagelet.mafengwo.cn/poi/pagelet/poiLocationApi"
    ticketsApi = "http://pagelet.mafengwo.cn/poi/pagelet/poiTicketsApi"
    keyConvert = {
        "交通": "transInfo", "门票": "ticketsInfo", "开放时间": "openInfo",
     }
    # 初始化方法
    def __init__(self, *, province='海南', saveMode='json'):
        # 文档字符串
        '''
        Initialize a new instance of the MafengwoSpider.

        :Args:
         - province : a str of Chinese province name which resorts are located
         in.
         - saveMode : a str of file type to store fetched resorts' data.
        '''
        # initialize variables
        self.links = list()
        self.data = list()
        self.provName = province
        self.saveMode = saveMode
        # create json file object:
        if not os.path.exists(savePath):
            os.makedirs(savePath)
        filePath = os.path.join(savePath, filename+'.'+self.saveMode)
        self.file = open(filePath, 'w', encoding='utf-8')


    # 爬虫主程序
    def run(self):
        # 文档字符串
        '''
        Main spider method of MafengwoSpider.

        Fetches all resorts links, parses every resort website according to their
        links then packes all dictionary formatted resorts' info data into a data
        list.
        '''
        # error counter variable
        num = 1
        # 方法实现
        self.getLinks()
        for link in self.links:
            print(f'>>>> getting resorts webpage:', link)
            html = self.getHTML(link).text
            time.sleep(random.randint(1,3))
            if html:
                print(f'>>>> Success getting resort {link}.')
                self.data.append(self.parseResort(html))
            else:
                print(f'>>>> Failure getting resort {link}.')
                # 防止网络不可靠情况下，爬虫一直运行下去：
                if num == 1:
                    num += 1
                    lastLink = self.links.index(link)
                else:
                    if self.links.index(link) != lastLink + 1:
                        num = 1
                    elif num <= 10:
                        num += 1
                        lastLink = self.links.index(link)
                    else:
                        raise ValueError('NetWork Unavailable!')
        print(len(self.links))
        print(len(self.data))
        # print(self.data)
        # print(len(self.links))
        # print(len(self.data))

    # HTTP请求页面方法
    def getHTML(self, url, **para):
        # 文档字符串
        '''
        Requests object website's HTML source code.

        If Timeout exception occured, retries HTTP Request 10 times;
        If retry exceeded 10 times or other exceptions occured, return None.

        :Args:
         - url : object website's url.
         - para : dictionary of URL parameters to append to the URL.

        :Returns:
         - html : a str of HTML code if request suceeded or None if exceptions
           occured.
        '''
        # 方法实现
        html = None
        error = False
        num = 1
        while True:
            try:
                response = requests.get(url, params=para, timeout=TIMEOUT,
                                             headers=random.choice(HEADERS))
                # print(response.encoding)
                response.raise_for_status()
                response.encoding = 'utf-8'
                html = response
                print('>> Request Webpage Success.')
            except requests.exceptions.Timeout:
                print(f'>> Timeout Occured: {num} times.')
                num += 1
                if num > 10:
                    print('>> Exceed Timeout maximum retry times.')
                    # 日志记录
                    error = True
            except requests.exceptions.RequestException as e:
                print('>> Exception Occured:', e)
                # 日志记录
                error = True
            finally:
                if html or error:
                    break
                # timeout retry pause.
                time.sleep(5)
        return html


    # 获取所有景点链接方法
    def getLinks(self, pStart=1, pEnd=50):
        # 文档字符串
        '''
        Fetches all resorts' links on Mafengwo website during given pages.

        Uses xpath to parse fetched websites' HTML and iterates parsed HTML
        elements, then updates the resorts' links container `self.links` if
        element text contains resort type word.

        :Args:
         - pStart : An int of starting website page.
         - pEnd : An int of ending website page.
        '''
        # error counter variable
        num = 1
        # 方法实现
        for page in range(pStart, pEnd+1):
            print(f'>>> Getting page {page}')
            html = self.getHTML(self.baseUrl, p=page, q=self.provName).text
            time.sleep(random.randint(1,3))
            if html:
                print(f'>>> Success getting page {page}.')
                selector = etree.HTML(html)
                elements = selector.xpath('//div[@class="att-list"]/ul/li/div/div[2]/h3/a')
                if len(elements) != 15:
                    raise ValueError('parse link error!')
                self.links.extend([e.get('href') for e in elements if '景点' in e.text])
            else:
                print(f'>>> Failure getting page {page}.')
                # 防止网络不可靠情况下，爬虫一直运行下去：
                if num == 1:
                    num += 1
                    lastPage = page
                else:
                    if page != lastPage + 1:
                        num = 1
                    elif num <= 10:
                        num += 1
                        lastPage = page
                    else:
                        raise ValueError('NetWork Unavailable!')


    # 解析景点数据方法
    def parseResort(self, html):
        # 文档字符串
        '''
        Parses given resort's info data, pack them into a dictionary and return
        dictionary formatted data.

        :Args:
         - html : a str of html source code of given resort.

        :Returns:
         - item : a dict of parsed resort's info data.
        '''
        # 方法实现
        item = {
            'resortName': None,
            'poi_id': None,
            'introduction': None,
            'areaName': None,
            'areaId': None,
            'address': None,
            'lat': None,
            'lng': None,
            'openInfo': None,
            'ticketsInfo': None,
            'transInfo': None,
            'tel': None,
            'item-site': None,
            'item-time': None,
            'payAbstracts': None,
            # administrative field:
            'source': 'mafengwo',
            'timeStamp': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }

        row_top, overview = etree.HTML(html).xpath(('//div[@class="row row-top" '
                                                    'or @data-anchor="overview"]'))

        mod_detail = overview.xpath('//div[@class="mod mod-detail"]')
        if len(mod_detail) == 1:
            for dl in mod_detail[0].xpath('dl'):
                dt, dd = dl
                # transform keys and insert key-value pair into dict.
                item[self.keyConvert.get(dt.text)] = dd.xpath('string()').strip()

            intro = mod_detail[0].xpath('div[@class="summary"]')
            if len(intro) == 1:
                item['introduction'] = intro[0].xpath('string()').strip()

            base_info = mod_detail[0].xpath('ul[@class="baseinfo clearfix"]')
            if len(base_info) == 1:
                for li in base_info[0].xpath('li'):
                    # print(li.get('class'))
                    content = li.xpath('div[@class="content"]').pop()
                    item[li.get('class')] = content.xpath('string()').strip()
        # 后面可以改改
        mod_location = overview.xpath('div[@class="mod mod-location"]').pop()
        poi = mod_location.xpath(('//div[contains(@data-api,"poiLocationApi")'
                                  ']/@data-params')).pop()
        apiData = self.getHTML(self.locationApi, params=poi).json()['data']
        a = row_top.xpath('//div[@class="drop"]/span/a').pop()

        item['resortName'] = row_top.xpath('//div[@class="title"]/h1/text()').pop()
        item['areaName'] = a.text
        item['areaId'] = int(re.search('(\d+)\.html', a.get('href'))[1])
        item['address'] = mod_location.xpath('//p[@class="sub"]/text()').pop()
        item['poi_id'] = int(json.loads(poi)['poi_id'])
        item['lat'] = apiData['controller_data']['poi']['lat']
        item['lng'] = apiData['controller_data']['poi']['lng']

        return item


    # 析构方法
    def __del__(self):
        # 文档字符串
        '''
        Save fetched resorts' data in json format, close spider and json file
        object.
        '''
        # 方法实现
        json.dump(self.data, self.file, ensure_ascii=False)
        self.file.close()


if __name__ == '__main__':
    spider = MafengwoSpider()
    spider.run()
