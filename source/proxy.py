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

from settings import TIMEOUT, PROXY_COUNT, PROXY_MAX

# 全局变量：
# TIMEOUT = (6, 6)


# 类定义：
class SpiderProxy(object):
    # 文档字符串
    '''
    SpiderProxy class allows users to use IPProxyPool proxy for their spider.

    :Usage:

    '''

    # 类静态成员定义
    api_url = "http://127.0.0.1:8000/"
    # 初始化方法
    def __init__(self):
        # 文档字符串
        '''
        Initialize a new instance of the SpiderProxy.
        '''
        # 方法实现
        self.proxies = list()
        self.counter = dict()
        self.get_proxy()


    # 请求IPProxyPool API方法
    def request_api(self, url, **para):
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
        str_proxies = None
        num = 1
        while True:
            try:
                response = requests.get(url, params=para, timeout=TIMEOUT)
                # print(response.encoding)
                response.raise_for_status()
                response.encoding = 'utf-8'
                str_proxies = response.text
                print('>> Request IPProxyPool API Success.')
            except requests.exceptions.Timeout:
                print(f'>> Timeout Occured: {num} times.')
                num += 1
                if num > 10:
                    print('>> Exceed Timeout maximum retry times.')
                    # 日志记录
                    raise RuntimeError('Exceed Timeout maximum retry times.')
            finally:
                if str_proxies:
                    break
                # timeout retry pause.
                # time.sleep(1)
        return json.loads(str_proxies)


    # 获取代理IP方法
    def get_proxy(self):
        # 文档字符串

        # 方法实现
        print('>>> getting proxies from IPProxyPool.')
        raw_proxies = list()
        ac_num = PROXY_COUNT
        for type_num in range(2):
            raw_proxies.extend(self.request_api(self.api_url, types=type_num,
                                                count=ac_num, country='国内'))
            print(f'>> acquired types = {type_num} proxies number:', len(raw_proxies))
            if len(raw_proxies) == PROXY_COUNT:
                break
            ac_num = PROXY_COUNT - len(raw_proxies)
        print('>>> acquired proxy number:', len(raw_proxies))

        for proxy in raw_proxies:
            url = '%s:%s' % (proxy[0], proxy[1])
            self.proxies.append(url)
            self.counter[url] = PROXY_MAX
        print(self.proxies)
        print(self.counter)
        print('>>> success getting proxies.')


    def delete_proxy(self, url):
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
        self.request_api(''.join([self.api_url, 'delete']), ip=ip)
        for i in range(len(self.proxies)-1, -1, -1):
            if self.proxies[i] == url:
                self.proxies.pop(i)
        self.counter.pop(url)
        print(self.proxies)
        print('>>> success deleting proxy:', url)


    def pop_proxy(self):
        # 文档字符串

        # 方法实现
        while True:
            if len(self.proxies) == 0:
                self.get_proxy()

            url = random.choice(self.proxies)
            self.counter[url] -= 1

            if self.counter[url] <= 0:
                self.delete_proxy(url)
            else:
                break

        return url


# 测试代码：
if __name__ == '__main__':
    proxyer = SpiderProxy()
    print(proxyer.pop_proxy())
