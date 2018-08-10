#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 模块字符串
'''
Define a SpiderProxy class allows users to use IPProxyPool API for their spider.
'''

# 导入模块：
import requests
import json
import random
# 全局变量：
TIMEOUT = (6, 6)
PROXY_COUNT = 20
PROXY_MAX = 60

# 类定义：
class SpiderProxy(object):
    # 文档字符串
    '''
    SpiderProxy class allows users to use IPProxyPool proxy for their spider.

    :Usage:

    '''

    # 类静态成员定义
    selectUrl = "http://127.0.0.1:8000/"
    deleteUrl = "http://127.0.0.1:8000/delete"
    # 初始化方法
    def __init__(self):
        # 文档字符串
        '''
        Initialize a new instance of the SpiderProxy.
        '''
        # 方法实现
        self.proxies = list()
        self.counter = dict()
        self.getProxy()


    # 请求IPProxyPool API方法
    def getApi(self, url, **para):
        # 文档字符串
        '''
        Sends HTTP Requests to IPProxyPool API.

        If Timeout exception occured, retries HTTP Request 10 times;
        If retry exceeded 10 times or other exceptions occured, raise exception.

        :Args:
         - url : a str of IPProxyPool API Url.
         - para : keyword arguments of Url Parametes to append to API Url.

        :Returns:
         - html : a json of IPProxyPool API data.
        '''
        # 方法实现
        strProxies = None
        num = 1
        while True:
            try:
                response = requests.get(url, params=para, timeout=TIMEOUT)
                # print(response.encoding)
                response.raise_for_status()
                response.encoding = 'utf-8'
                strProxies = response.text
                print('>> Request IPProxyPool API Success.')
            except requests.exceptions.Timeout:
                print(f'>> Timeout Occured: {num} times.')
                num += 1
                if num > 10:
                    print('>> Exceed Timeout maximum retry times.')
                    # 日志记录
                    raise RuntimeError('Exceed Timeout maximum retry times.')
            finally:
                if strProxies:
                    break
                # timeout retry pause.
                # time.sleep(1)
        return json.loads(strProxies)


    # 获取代理IP方法
    def getProxy(self):
        # 文档字符串

        # 方法实现
        print('>>> getting proxies from IPProxyPool.')
        rawProxies = list()
        acNum = PROXY_COUNT
        for typeNum in range(2):
            rawProxies.extend(self.getApi(self.selectUrl, types=typeNum, count=acNum, country='国内'))
            print(f'>> acquired types = {typeNum} proxies number:', len(rawProxies))
            if len(rawProxies) == PROXY_COUNT:
                break
            acNum = PROXY_COUNT - len(rawProxies)
        print('>>> acquired proxy number:', len(rawProxies))

        for proxy in rawProxies:
            url = '%s:%s' % (proxy[0], proxy[1])
            self.proxies.append(url)
            self.counter[url] = 0
        print(self.proxies)
        print(self.counter)
        print('>>> success getting proxies.')


    def delProxy(self, url):
        # 文档字符串
        '''
        Delete an unavailable ip from IPProxyPool API and pop its counterpart
        from proxies list and counter dict.

        :Args:
         - url : a str of url composed of ip and port.
        '''
        # 方法实现
        print('>>> delete proxy:', url)
        ip= url.split(':')[0]
        print('>>> delete ip:', ip)
        self.getApi(self.deleteUrl, ip=ip)
        for i in range(len(self.proxies)-1, -1, -1):
            if self.proxies[i] == url:
                self.proxies.pop(i)
        self.counter.pop(url)
        print(self.proxies)
        print('>>> success deleting proxy:', url)

    def popProxy(self):
        # 文档字符串

        # 方法实现
        if len(self.proxies) == 0:
            self.getProxy()

        url = random.choice(self.proxies)
        self.counter[url] += 1

        if self.counter[url] > PROXY_MAX:
            self.delProxy(url)

        return url


# 测试代码：
if __name__ == '__main__':
    proxyer = SpiderProxy()
