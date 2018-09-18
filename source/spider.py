#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
Define a BaseSpider class allows you to fetch all resorts infos in
given province.
'''

import os
import re
import json
import time
import random

import requests
from lxml import etree
from proxy import SpiderProxy
from requests.exceptions import ProxyError, HTTPError, RequestException, \
                                Timeout, ReadTimeout, TooManyRedirects

from settings import PROXY_PUNISH, USER_AGENTS, TIMEOUT, save_path, file_name
# 全局变量定义


# 类定义：

# 旅游爬虫基类：
class BaseSpider(object):
    # 文档字符串
    '''
    BaseSpider class allows users to fetch all data from different websites.

    :Usage:

    '''
    # 类静态成员定义
    SAVE_MODES = ('json', 'txt')
    # 初始化方法
    def __init__(self, area_name='海南'):
        # 文档字符串
        '''
        Initialize a new instance of the BaseSpider.

        :Args:
         - area_name : a str of Chinese area name which data are located
         in.

        '''
        # 方法实现
        self.area_name = area_name
        self.data = list()

        # 初始化爬虫代理
        self.proxyer = SpiderProxy()


    # HTTP请求头配置方法
    def config_header(self, host):
        pass


    #  HTTP请求代理配置方法
    def config_proxy(self):
        self.proxy_url = self.proxyer.pop_proxy()
        print('> proxy:', self.proxy_url)
        return {
            'http': 'http://' + self.proxy_url,
            'https': 'https://' + self.proxy_url
         }



    # 数据存储方法
    def dump_data(self, save_mode='json'):
        # 文档字符串
        '''
        Dump spider fetched data into a file specified by `save_mode` para.

        :Args:
         - save_mode : file type to save spider fectched data.

        '''
        # 方法实现
        if save_mode not in self.SAVE_MODES:
            raise RuntimeError('存储模式指定有误，请输入txt、json')
        # create json file object:
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        file_path = os.path.join(save_path, file_name+'.'+save_mode)
        if save_mode == 'json':
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(self.data, file, ensure_ascii=False)
        else:
            # 此处可以拓展其他文件存储类型
            pass


    # HTTP请求页面方法
    def request_html(self, method, url, **kwargs):
        # 文档字符串
        '''
        Requests website's HTML source code.

        If Timeout, ProxyError, HTTPError, ReadTimeout, TooManyRedirects
        exception occured, retries HTTP Request 10 times; If retry exceeded 10
        times or other exceptions occured, return None.

        :Args:
         - method : method for new HTTP Requests supported by the :class`Request`
           object in `requests` module.
         - url : URL for new HTTP Requests supported by the :class`Request` object
           in `requests` module.
         - **kwargs : key words arguments supported by the :class:`Request` object
           in `requests` module.

        :Returns:
         - html : a :class:`Response` if request suceeded or None if exceptions
           occured.

        '''
        # 方法实现
        # html = None
        error = False
        num = 1
        while True:
            try:
                response = requests.request(method, url,
                                            proxies=self.config_proxy(),
                                            **kwargs)
                # print(response.encoding)
                response.raise_for_status()
                response.encoding = 'utf-8'
                # html = response
                print('>> Request Webpage Success.')
            except (Timeout, ProxyError, HTTPError,
                    ReadTimeout, TooManyRedirects) as e:

                print('>> Exceptions Occured:', e)
                print(f'>> Retries {num} times.')
                response = None
                self.proxyer.counter[self.proxy_url] -= PROXY_PUNISH
                num += 1
                if num > 10:
                    print('>> Exceed maximum retry times.')
                    # 日志记录
                    error = True
            except RequestException as e:
                print('>> Exception Occured:', e)
                # 日志记录
                response = None
                self.proxyer.counter[self.proxy_url] -= PROXY_PUNISH
                error = True
            finally:
                if response or error:   # 原来的response是html
                    break
                # timeout retry pause.
                # time.sleep(1)
                # time.sleep(random.randint(1,3))
        return response


# 马蜂窝旅游爬虫子类：
class MafengwoSpider(BaseSpider):
    # 文档字符串
    '''
    MafengwoSpider class allows users to fetch all data from mafengwo websites.

    :Usage:

    '''
    # 类静态成员定义
    base_url = "http://www.mafengwo.cn/search/s.php?t=poi&kt=1"
    location_api = "http://pagelet.mafengwo.cn/poi/pagelet/poiLocationApi"
    # tickets_api = "http://pagelet.mafengwo.cn/poi/pagelet/poiTicketsApi"
    req_host = {"www": "www.mafengwo.cn", "pagelet": "pagelet.mafengwo.cn"}
    key_convert = {
        "交通": "transInfo", "门票": "ticketsInfo", "开放时间": "openInfo",
     }


    # 初始化方法
    def __init__(self, area_name='海南'):
        super(MafengwoSpider, self).__init__(area_name)
        self.links = list()


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
        self.get_links()
        for link in self.links:
            print(f'>>>> getting resorts webpage:', link)
            html = self.request_html('GET', link, timeout=TIMEOUT,
                                     headers=self.config_header('www'))
            # time.sleep(1)
            # time.sleep(random.randint(1,3))
            if html:
                while True:
                    test = etree.HTML(html.text).xpath(('//div[@class="row row-top" '
                                                        'or @data-anchor="overview"]'))
                    if len(test) == 2:
                        print(f'>>>> Success getting resort {link}.')
                        self.data.append(self.parse_resort(html.text))
                        break
                    # 走到这里的时候说明代理ip被禁了，换新ip重新请求一次
                    # 相信代理ip池中一定有可靠ip，因此不会出现死循环
                    self.proxyer.counter[self.proxy_url] -= PROXY_PUNISH
                    print('>>>> getting wrong resort content. Retries again!')
                    html = self.request_html('GET', link, timeout=TIMEOUT,
                                             headers=self.config_header('www'))
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

        self.dump_data('json')
        # print(self.data)
        # print(len(self.links))
        # print(len(self.data))


    # HTTP请求头配置方法
    def config_header(self, host_key):
        # 文档字符串
        '''
        Loads Mafengwo's HTTP Requests Header with random User-Agents.

        :Args:
         - host_key : a str of Host's key.
           www - www.mafengwo.cn; pagelet - pagelet.mafengwo.cn
        :Returns:
         a dict of HTTP Requests Header.
        '''
        # 方法实现
        # 可以使用python第三方库fake-useragent实现随机user-agent
        useragent = random.choice(USER_AGENTS)
        print('> user agent:', useragent)
        return {
            'Accept': ('text/html,application/xhtml+xml,application/xml;'
                       'q=0.9,image/webp,image/apng,*/*;q=0.8'),
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Host': self.req_host[host_key],
            'User-Agent': useragent,
            'Proxy-Connection': 'keep-alive',
        }


    # 获取所有景点链接方法
    def get_links(self, pStart=1, pEnd=50):
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
            req_param = {'p': page, 'q': self.area_name}
            html = self.request_html('GET', self.base_url, params=req_param,
                                            timeout=TIMEOUT,
                                            # proxies=self.config_proxy(),
                                            headers=self.config_header('www'))
            # time.sleep(random.randint(1,3))
            # time.sleep(1)
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
                    self.proxyer.counter[self.proxy_url] -= PROXY_PUNISH
                    print('>>> getting wrong page content. Retrise again!')
                    html = self.request_html('GET', self.base_url, params=req_param,
                                                    timeout=TIMEOUT,
                                                    # proxies=self.config_proxy(),
                                                    headers=self.config_header('www'))
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
        # print(self.links)


    # 解析景点数据方法
    def parse_resort(self, html):
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
            'item_site': None,
            'item_time': None,
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
                item[self.key_convert.get(dt.text)] = dd.xpath('string()').strip()

            intro = mod_detail[0].xpath('div[@class="summary"]')
            if len(intro) == 1:
                item['introduction'] = intro[0].xpath('string()').strip()

            base_info = mod_detail[0].xpath('ul[@class="baseinfo clearfix"]')
            if len(base_info) == 1:
                for li in base_info[0].xpath('li'):
                    # print(li.get('class'))
                    content = li.xpath('div[@class="content"]').pop()
                    item[li.get('class').replace('-', '_')] = content.xpath('string()').strip()
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
                response = self.request_html('GET', self.location_api,
                                             params={'params': poi},
                                             timeout=TIMEOUT,
                                             # proxies=self.config_proxy(),
                                             headers=self.config_header('pagelet'))
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


# class MafengwoSpider(object):
#     # 文档字符串
#     '''
#     MafengwoSpider class allows users to fetch all data from mafengwo websites.
#
#     :Usage:
#
#     '''
#     # 爬虫静态成员定义
#     base_url = "http://www.mafengwo.cn/search/s.php?t=poi&kt=1"
#     location_api = "http://pagelet.mafengwo.cn/poi/pagelet/poilocation_api"
#     tickets_api = "http://pagelet.mafengwo.cn/poi/pagelet/poitickets_api"
#     Host = ['www.mafengwo.cn', 'pagelet.mafengwo.cn']
#     key_convert = {
#         "交通": "transInfo", "门票": "ticketsInfo", "开放时间": "openInfo",
#      }
#     # 初始化方法
#     def __init__(self, *, province='海南', save_mode='json'):
#         # 文档字符串
#         '''
#         Initialize a new instance of the MafengwoSpider.
#
#         :Args:
#          - province : a str of Chinese province name which resorts are located
#          in.
#          - save_mode : a str of file type to store fetched resorts' data.
#         '''
#         # initialize variables
#         self.links = list()
#         self.data = list()
#         self.prov_name = province
#         self.save_mode = save_mode
#
#         # init proxy
#         # self.proxyer = SpiderProxy()
#
#     # 爬虫主程序
#     def run(self):
#         # 文档字符串
#         '''
#         Main spider method of MafengwoSpider.
#
#         Fetches all resorts links, parses every resort website according to their
#         links then packes all dictionary formatted resorts' info data into a data
#         list.
#         '''
#         # error counter variable
#         start = time.time()
#         num = 1
#         # 方法实现
#         self.getLinks()
#         for link in self.links:
#             print(f'>>>> getting resorts webpage:', link)
#             html = self.getHTML(link, 0)
#             time.sleep(1)
#             # time.sleep(random.randint(1,3))
#             if html:
#                 while True:
#                     test = etree.HTML(html.text).xpath(('//div[@class="row row-top" '
#                                                         'or @data-anchor="overview"]'))
#                     if len(test) == 2:
#                         print(f'>>>> Success getting resort {link}.')
#                         self.data.append(self.parseResort(html.text))
#                         break
#                     # 走到这里的时候说明代理ip被禁了，换新ip重新请求一次
#                     # 相信代理ip池中一定有可靠ip，因此不会出现死循环
#                     print('>>>> getting wrong resort content. Retries again!')
#                     html = self.getHTML(link, 0)
#             else:
#                 print(f'>>>> Failure getting resort {link}.')
#                 # 防止网络不可靠情况下，爬虫一直运行下去：
#                 if num == 1:
#                     num += 1
#                     lastLink = self.links.index(link)
#                 else:
#                     if self.links.index(link) != lastLink + 1:
#                         num = 1
#                     elif num <= 10:
#                         num += 1
#                         lastLink = self.links.index(link)
#                     else:
#                         raise ValueError('NetWork Unavailable!')
#         print(len(self.links))
#         print(len(self.data))
#         end = time.time()
#         print(end-start)
#         # print(self.data)
#         # print(len(self.links))
#         # print(len(self.data))
#
#
#     def getHeader(self, hostTypes):
#         # 文档字符串
#         '''
#         Loads HTTP Requests Header with random User-Agents.
#
#         :Args:
#          - hostTypes : a int of Host element index number.
#            0 - www.mafengwo.cn; 1- pagelet.mafengwo.cn
#         :Returns:
#          a dict of HTTP Requests Header.
#         '''
#         # 方法实现
#         useragent = random.choice(USER_AGENTS)
#         print('> user agent:', useragent)
#         return {
#             'Accept': ('text/html,application/xhtml+xml,application/xml;'
#                        'q=0.9,image/webp,image/apng,*/*;q=0.8'),
#             'Accept-Encoding': 'gzip, deflate',
#             'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
#             'Host': self.Host[hostTypes],
#             'User-Agent': useragent,
#             'Proxy-Connection': 'keep-alive',
#         }
#
#
#     def getProxies(self):
#         url = self.proxyer.popProxy()
#         print('> proxy:', url)
#         return {
#             'http': 'http://' + url,
#             'https': 'https://' + url
#          }
#
#
#     # HTTP请求页面方法
#     def getHTML(self, url, hostTypes, **para):
#         # 文档字符串
#         '''
#         Requests object website's HTML source code.
#
#         If Timeout exception occured, retries HTTP Request 10 times;
#         If retry exceeded 10 times or other exceptions occured, return None.
#
#         :Args:
#          - url : object website's url.
#          - hostTypes : a int of Host element index number.
#            0 - www.mafengwo.cn; 1- pagelet.mafengwo.cn
#          - para : dictionary of URL parameters to append to the URL.
#
#         :Returns:
#          - html : a str of HTML code if request suceeded or None if exceptions
#            occured.
#         '''
#         # 方法实现
#         html = None
#         error = False
#         num = 1
#         while True:
#             try:
#                 response = requests.get(url, params=para, timeout=TIMEOUT,
#                                              # proxies=self.getProxies(),
#                                              headers=self.getHeader(hostTypes))
#                 # print(response.encoding)
#                 response.raise_for_status()
#                 response.encoding = 'utf-8'
#                 html = response
#                 print('>> Request Webpage Success.')
#             except (Timeout, ProxyError, HTTPError, ReadTimeout, TooManyRedirects) as e:
#                 print(f'>> Exceptions Occured:', e)
#                 print(f'>> Retries {num} times.')
#                 num += 1
#                 if num > 10:
#                     print('>> Exceed maximum retry times.')
#                     # 日志记录
#                     error = True
#             except RequestException as e:
#                 print('>> Exception Occured:', e)
#                 # 日志记录
#                 error = True
#             finally:
#                 if html or error:
#                     break
#                 # timeout retry pause.
#                 # time.sleep(1)
#                 # time.sleep(random.randint(1,3))
#         return html
#
#
#     # 获取所有景点链接方法
#     def getLinks(self, pStart=1, pEnd=50):
#         # 文档字符串
#         '''
#         Fetches all resorts' links on Mafengwo website during given pages.
#
#         Uses xpath to parse fetched websites' HTML and iterates parsed HTML
#         elements, then updates the resorts' links container `self.links` if
#         element text contains resort type word.
#
#         :Args:
#          - pStart : An int of starting website page.
#          - pEnd : An int of ending website page.
#         '''
#         # error counter variable
#         num = 1
#         # 方法实现
#         for page in range(pStart, pEnd+1):
#             print(f'>>> Getting page {page}')
#             html = self.getHTML(self.base_url, 0, p=page, q=self.prov_name)
#             # time.sleep(random.randint(1,3))
#             # time.sleep(1)
#             if html:
#                 while True:
#                     selector = etree.HTML(html.text)
#                     elements = selector.xpath('//div[@class="att-list"]/ul/li/div/div[2]/h3/a')
#                     print('>>> links count:', len(elements))
#                     if len(elements) == 15:
#                         print(f'>>> Success getting page {page}.')
#                         self.links.extend([e.get('href') for e in elements if '景点' in e.text])
#                         break
#                     # 走到这里的时候说明代理ip被禁了，换新ip重新请求一次
#                     # 相信代理ip池中一定有可靠ip，因此不会出现死循环
#                     print('>>> getting wrong page content. Retrise again!')
#                     html = self.getHTML(self.base_url, 0, p=page, q=self.prov_name)
#             else:
#                 print(f'>>> Failure getting page {page}.')
#                 # 防止网络不可靠情况下，爬虫一直运行下去：
#                 if num == 1:
#                     num += 1
#                     lastPage = page
#                 else:
#                     if page != lastPage + 1:
#                         num = 1
#                     elif num <= 10:
#                         num += 1
#                         lastPage = page
#                     else:
#                         raise ValueError('NetWork Unavailable!')
#
#
#     # 解析景点数据方法
#     def parseResort(self, html):
#         # 文档字符串
#         '''
#         Parses given resort's info data, pack them into a dictionary and return
#         dictionary formatted data.
#
#         :Args:
#          - html : a str of html source code of given resort.
#
#         :Returns:
#          - item : a dict of parsed resort's info data.
#         '''
#         # 方法实现
#         print('>>> start parsing resort.')
#         item = {
#             'resortName': None,
#             'poi_id': None,
#             'introduction': None,
#             'areaName': None,
#             'areaId': None,
#             'address': None,
#             'lat': None,
#             'lng': None,
#             'openInfo': None,
#             'ticketsInfo': None,
#             'transInfo': None,
#             'tel': None,
#             'item_site': None,
#             'item_time': None,
#             'payAbstracts': None,
#             # administrative field:
#             'source': 'mafengwo',
#             'timeStamp': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#         }
#
#         row_top, overview = etree.HTML(html).xpath(('//div[@class="row row-top" '
#                                                     'or @data-anchor="overview"]'))
#
#         mod_detail = overview.xpath('//div[@class="mod mod-detail"]')
#         if len(mod_detail) == 1:
#             for dl in mod_detail[0].xpath('dl'):
#                 dt, dd = dl
#                 # transform keys and insert key-value pair into dict.
#                 item[self.key_convert.get(dt.text)] = dd.xpath('string()').strip()
#
#             intro = mod_detail[0].xpath('div[@class="summary"]')
#             if len(intro) == 1:
#                 item['introduction'] = intro[0].xpath('string()').strip()
#
#             base_info = mod_detail[0].xpath('ul[@class="baseinfo clearfix"]')
#             if len(base_info) == 1:
#                 for li in base_info[0].xpath('li'):
#                     # print(li.get('class'))
#                     content = li.xpath('div[@class="content"]').pop()
#                     item[li.get('class').replace('-', '_')] = content.xpath('string()').strip()
#         # 后面可以改改
#         a = row_top.xpath('//div[@class="drop"]/span/a').pop()
#         item['resortName'] = row_top.xpath('//div[@class="title"]/h1/text()').pop()
#         item['areaName'] = a.text
#         item['areaId'] = int(re.search('(\d+)\.html', a.get('href'))[1])
#
#         mod_location = overview.xpath('div[@class="mod mod-location"]').pop()
#         poi = mod_location.xpath(('//div[contains(@data-api,"poilocation_api")'
#                                   ']/@data-params')).pop()
#         while True:
#             try:
#                 response = self.getHTML(self.location_api, 1, params=poi)
#                 apiData = response.json()['data']
#             except:
#                 print('>> acquired location fail! Retries Again.')
#             else:
#                 break
#         item['address'] = mod_location.xpath('//p[@class="sub"]/text()').pop()
#         item['poi_id'] = int(json.loads(poi)['poi_id'])
#         item['lat'] = apiData['controller_data']['poi']['lat']
#         item['lng'] = apiData['controller_data']['poi']['lng']
#
#         print('>>> end parsing resort.')
#         return item
#
#
#     # 析构方法
#     def __del__(self):
#         # 文档字符串
#         '''
#         Save fetched resorts' data in json format, close spider and json file
#         object.
#         '''
#         # 方法实现
#         json.dump(self.data, self.file, ensure_ascii=False)
#         self.file.close()


if __name__ == '__main__':
    spider = MafengwoSpider()
    spider.run()
