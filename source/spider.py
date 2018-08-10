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
from proxy import SpiderProxy
from requests.exceptions import Timeout, ProxyError, HTTPError, RequestException, ReadTimeout, TooManyRedirects

# 全局变量定义
# HTTP请求配置变量：
USER_AGENTS = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
]

TIMEOUT = (5, 5)


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
    Host = ['www.mafengwo.cn', 'pagelet.mafengwo.cn']
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
        # init proxy
        self.proxyer = SpiderProxy()

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
        start = time.time()
        num = 1
        # 方法实现
        self.getLinks()
        for link in self.links:
            print(f'>>>> getting resorts webpage:', link)
            html = self.getHTML(link, 0)
            time.sleep(1)
            # time.sleep(random.randint(1,3))
            if html:
                while True:
                    test = etree.HTML(html.text).xpath(('//div[@class="row row-top" '
                                                        'or @data-anchor="overview"]'))
                    if len(test) == 2:
                        print(f'>>>> Success getting resort {link}.')
                        self.data.append(self.parseResort(html.text))
                        break
                    # 走到这里的时候说明代理ip被禁了，换新ip重新请求一次
                    # 相信代理ip池中一定有可靠ip，因此不会出现死循环
                    print('>>>> getting wrong resort content. Retries again!')
                    html = self.getHTML(link, 0)
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
        end = time.time()
        print(end-start)
        # print(self.data)
        # print(len(self.links))
        # print(len(self.data))


    def getHeader(self, hostTypes):
        # 文档字符串
        '''
        Loads HTTP Requests Header with random User-Agents.

        :Args:
         - hostTypes : a int of Host element index number.
           0 - www.mafengwo.cn; 1- pagelet.mafengwo.cn
        :Returns:
         a dict of HTTP Requests Header.
        '''
        # 方法实现
        useragent = random.choice(USER_AGENTS)
        print('> user agent:', useragent)
        return {
            'Accept': ('text/html,application/xhtml+xml,application/xml;'
                       'q=0.9,image/webp,image/apng,*/*;q=0.8'),
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Host': self.Host[hostTypes],
            'User-Agent': useragent,
            'Proxy-Connection': 'keep-alive',
        }

    def getProxies(self):
        url = self.proxyer.popProxy()
        print('> proxy:', url)
        return {
            'http': 'http://' + url,
            'https': 'https://' + url
         }


    # HTTP请求页面方法
    def getHTML(self, url, hostTypes, **para):
        # 文档字符串
        '''
        Requests object website's HTML source code.

        If Timeout exception occured, retries HTTP Request 10 times;
        If retry exceeded 10 times or other exceptions occured, return None.

        :Args:
         - url : object website's url.
         - hostTypes : a int of Host element index number.
           0 - www.mafengwo.cn; 1- pagelet.mafengwo.cn
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
                                             proxies = self.getProxies(),
                                             headers=self.getHeader(hostTypes))
                # print(response.encoding)
                response.raise_for_status()
                response.encoding = 'utf-8'
                html = response
                print('>> Request Webpage Success.')
            except (Timeout, ProxyError, HTTPError, ReadTimeout, TooManyRedirects) as e:
                print(f'>> Exceptions Occured:', e)
                print(f'>> Retries {num} times.')
                num += 1
                if num > 10:
                    print('>> Exceed maximum retry times.')
                    # 日志记录
                    error = True
            except RequestException as e:
                print('>> Exception Occured:', e)
                # 日志记录
                error = True
            finally:
                if html or error:
                    break
                # timeout retry pause.
                # time.sleep(1)
                time.sleep(random.randint(1,3))
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
            html = self.getHTML(self.baseUrl, 0, p=page, q=self.provName)
            # time.sleep(random.randint(1,3))
            time.sleep(1)
            if html:
                while True:
                    selector = etree.HTML(html.text)
                    elements = selector.xpath('//div[@class="att-list"]/ul/li/div/div[2]/h3/a')
                    print('>>> links count:', len(elements))
                    if len(elements) == 15:
                        print(f'>>> Success getting page {page}.')
                        self.links.extend([e.get('href') for e in elements if '景点' in e.text])
                        break
                    # 走到这里的时候说明代理ip被禁了，换新ip重新请求一次
                    # 相信代理ip池中一定有可靠ip，因此不会出现死循环
                    print('>>> getting wrong page content. Retrise again!')
                    html = self.getHTML(self.baseUrl, 0, p=page, q=self.provName)
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
        print('>>> start parsing resort.')
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
        a = row_top.xpath('//div[@class="drop"]/span/a').pop()
        item['resortName'] = row_top.xpath('//div[@class="title"]/h1/text()').pop()
        item['areaName'] = a.text
        item['areaId'] = int(re.search('(\d+)\.html', a.get('href'))[1])

        mod_location = overview.xpath('div[@class="mod mod-location"]').pop()
        poi = mod_location.xpath(('//div[contains(@data-api,"poiLocationApi")'
                                  ']/@data-params')).pop()
        while True:
            try:
                response = self.getHTML(self.locationApi, 1, params=poi)
                apiData = response.json()['data']
            except:
                print('>> acquired location fail! Retries Again.')
            else:
                break
        item['address'] = mod_location.xpath('//p[@class="sub"]/text()').pop()
        item['poi_id'] = int(json.loads(poi)['poi_id'])
        item['lat'] = apiData['controller_data']['poi']['lat']
        item['lng'] = apiData['controller_data']['poi']['lng']

        print('>>> end parsing resort.')
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
