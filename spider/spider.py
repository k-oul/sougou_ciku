# coding: utf-8

'''
Author K_oul
'''

import requests
import time
import re
import os
from mysql_db import MysqlDB
from my_logger import MyLogger
from pyquery import PyQuery as pq

start_url = 'https://pinyin.sogou.com/dict/cate/index'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
types = ['城市信息','自然科学', '社会科学','工程应用','农林畜牧','医学医药','电子游戏','艺术设计','生活百科','运动休闲','人文科学','娱乐休闲']
db = MysqlDB()

def get_html(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return -1

class GetCate():

    def parse_index(self,html):
        doc = pq(html)
        items = doc('#dict_nav_list ul li a').items()
        result = []
        for item in items:
            url = 'https://pinyin.sogou.com' + item.attr('href')
            result.append(url)
        return result

    def cate_index(self, html, type1):
        doc = pq(html)
        items = doc('.cate_no_child.no_select a').items()
        result = []
        for item in items:
            url = 'http://pinyin.sogou.com' + item.attr('href') + '/default/{}'
            type2 = item.text().split('(')[0]
            try:
                page = (int(item.text().split('(')[1][:-1]) + 10 - 1) // 10
            except:
                page = 1
            result.append({
                'url': url,
                'page': str(page),
                'type1': type1,
                'type2': type2,
            })
        return result

    def start(self):
        html = get_html(start_url)
        type1_urls = self.parse_index(html)
        type1_map = list(zip(type1_urls, types))
        for url, type1  in type1_map:
            html = get_html(url)
            results = self.cate_index(html, type1)
            for result in results:
                db.save_one_data('sougou_cate', result)
            time.sleep(3)

class CateDetail():
    def __init__(self):
        self.log = MyLogger('SougouSpider',
                              os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log_SougouSpider.log'))

    def cate_detail(self, html,type1, type2):
        result = list()
        pattern = re.compile(
                '<div class="detail_title">.*?>(.*?)</a></div>.*?"dict_dl_btn"><a href="(.*?)"></a></div>', re.S)
        items = re.findall(pattern, html)
        for item in items:
            name,url = item
            result.append({
                'name': type1 + '_' + type2 + '_' + name,
                'url': url,
                'type1':type1,
                'type2': type2
            })
        return result

    def start(self):
        cate_list = db.find_all('sougou_cate')
        for cate in cate_list:
            type1 = cate['type1']
            type2 = cate['type2']
            n = int(cate['page'])
            for i in range(1, n + 1):
                print('正在解析{}的第{}页'.format(type1 + type2, i))
                url = cate['url'].format(i)
                html = get_html(url)
                if html != -1:
                    result = self.cate_detail(html, type1, type2)
                    self.log.info('正在解析页面{}'.format(url))
                    for data in result:
                        db.save_one_data('sougou_detail', data)
                        self.log.info('正在存储数据{}'.format(data['name']))
            time.sleep(2)


if __name__ == '__main__':
    # GetCate().start()
    CateDetail().start()