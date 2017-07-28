# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Spider
from weibo_spider.items import userinfoItem, tweetItem
import json
import pymysql
import time
import threading

db_user = 'industry'
db_pass = '123456industry'
db_host = '192.168.2.6'
db_port = '3306'
db_name = 'dingfu_industry'

kind = 'tweets'

increment = 1


class WeiboSpider(Spider):
    # handle_httpstatus_list = [403, 414]

    name = 'weibo'
    allowed_domains = ['m.weibo.cn']

    # src:商界名人
    src_user_uid = []
    src_user_follow_cnt = []

    continue_flag = 1

    followed_by_src_uid = []
    followed_by_src_follow_cnt = []

    src_follower_uid = []

    url_uid = ''

    cookie = {'_T_WM': 'd3422ad0e1bc42791fbd8462e27c2fd4',
              'SUB': '_2A250bF2tDeThGeNH4lcV9CbPyDiIHXVXr2PlrDV6PUJbkdBeLVKnkW1uIE4GMz4kB1dXYpBpIfxeqrsa9w..',
              'SUHB': '02M_PWHuDJImsI',
              'SCF': 'AuYUw4m07Rp7GFLPtiw5HZ1HLu1suKfD6hWBg11KZnyMnVJDwWMHF8PbdQeACTSkSVuDJ0HOwDWv0l7Vr5MSMv8.',
              'SSOLoginState': '1499999741',
              'H5_INDEX': '2',
              'M_WEIBOCN_PARAMS': 'featurecode%3D20000180%26oid%3D3905154304528573%26fid%3D1087030002_892_1003_0%26uicode%3D10000011'
              }

    def read_src_from_db(self):
        connect = pymysql.connect(user=db_user, passwd=db_pass, db=db_name, host=db_host, charset="utf8",
                                  use_unicode=True)
        cursor = connect.cursor()
        # uid,screen_name,gender,verified_reason,follow_cnt,followers_cnt,statuses_cnt
        cursor.execute("SELECT * FROM df_weibo_src_test")
        data = cursor.fetchall()
        for line in data:
            self.src_user_uid.append(line[1])
            self.src_user_follow_cnt.append(line[5])
            pass
        pass

    def read_followed_from_db(self):
        connect = pymysql.connect(user=db_user, passwd=db_pass, db=db_name, host=db_host, charset="utf8",
                                  use_unicode=True)
        cursor = connect.cursor()
        # uid,screen_name,gender,verified_reason,follow_cnt,followers_cnt,statuses_cnt
        cursor.execute("SELECT * FROM df_weibo_followed_by_src_test")
        data = cursor.fetchall()
        for line in data:
            self.followed_by_src_uid.append(line[1])
            pass
        pass

    def read_follower_from_db(self):
        connect = pymysql.connect(user=db_user, passwd=db_pass, db=db_name, host=db_host, charset="utf8",
                                  use_unicode=True)
        cursor = connect.cursor()
        # uid,screen_name,gender,verified_reason,follow_cnt,followers_cnt,statuses_cnt
        cursor.execute("SELECT * FROM df_weibo_src_follower_test")
        data = cursor.fetchall()
        for line in data:
            self.src_follower_uid.append(line[1])
            pass
        pass

    def parse_user_info(self, response):
        if self.url_uid not in response._url:
            # multi process communication
            return
        if u'cards' not in json.loads(response.body.decode('utf-8')):
            if self.url_uid in response._url:
                self.continue_flag = 0
            return
        if not len(json.loads(response.body.decode('utf-8'))[u'cards']):
            if self.url_uid in response._url:
                self.continue_flag = 0
            return
        if response.status == 414:
            if self.url_uid in response._url:
                self.continue_flag = 0
                pass
            return
        item = userinfoItem()
        item['response'] = json.loads(response.body.decode('utf-8'))
        item['kind'] = kind
        yield item
        pass

    def parse_tweets(self, response):
        if self.url_uid not in response._url:
            # multi process communication
            return
        r=json.loads(response.body.decode('utf-8'))
        if u'cards' not in r:
            if self.url_uid in response._url:
                self.continue_flag = 0
            return
        if not len(r[u'cards']):
            if self.url_uid in response._url:
                self.continue_flag = 0
            return
        if increment:
            if u'今天' not in r and u'前' not in r:
                if self.url_uid in response._url:
                    self.continue_flag = 0
                return
        if response.status == 414:
            if self.url_uid in response._url:
                self.continue_flag = 0
            return
        item = tweetItem()
        item['response'] = json.loads(response.body.decode('utf-8'))
        yield item
        pass

    def start_requests(self):
        # src part
        if kind == 'src_pos':
            url_head = [
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1001_0&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1001_1&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1001_2&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1001_3&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1001_4&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1001_5&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page='
            ]
            url_head_num = 0
            while url_head_num < len(url_head):
                self.continue_flag = 1
                url_page = 0
                while self.continue_flag:
                    url_page += 1
                    self.url_uid = url_head[url_head_num]
                    url = url_head[url_head_num] + str(url_page)
                    yield scrapy.Request(url=url, callback=self.parse_user_info, cookies=self.cookie)
                    if not self.continue_flag:
                        break
                url_head_num += 1

        # src part
        if kind == 'src_neg':
            url_head = [
                # 娱乐明星
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_0&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_1&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_2&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_3&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_4&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_5&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_6&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_7&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1003_8&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                # 知名作家
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_0&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_1&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_2&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_3&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_4&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_5&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_6&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_7&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1005_8&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                # 体育明星
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_0&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_1&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_2&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_3&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_4&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_5&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_6&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_7&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1002_8&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                # 传媒精英
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1007_0&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1007_1&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1007_2&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1007_3&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1007_4&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1007_5&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                # 政府官员
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_0&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_1&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_2&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_3&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_4&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_5&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_6&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_7&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
                'https://m.weibo.cn/api/container/getIndex?containerid=1087030002_892_1004_8&fearurecode=10000233&luicode=10000011&lfid=1087030002_817&featurecode=20000180&page=',
            ]
            url_head_num = 0
            while url_head_num < len(url_head):
                self.continue_flag = 1
                url_page = 0
                while self.continue_flag:
                    url_page += 1
                    self.url_uid = url_head[url_head_num]
                    url = url_head[url_head_num] + str(url_page)
                    yield scrapy.Request(url=url, callback=self.parse_user_info, cookies=self.cookie)
                    if not self.continue_flag:
                        break
                url_head_num += 1


        elif kind == 'followed_by_src':
            # followed_by_src part
            self.read_src_from_db()
            src_user_number = 0
            # for all src's
            while src_user_number < len(self.src_user_uid):
                self.continue_flag = 1
                url_page = 0
                self.url_uid = self.src_user_uid[src_user_number]
                url_head = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_%s&luicode=10000011&lfid=100505%s&featurecode=20000320&page=' % (
                    self.url_uid, self.url_uid)
                # each src, for all followers
                while self.continue_flag:
                    url_page += 1
                    url = url_head + str(url_page)
                    yield scrapy.Request(url=url, callback=self.parse_user_info)
                    if not self.continue_flag:
                        break
                src_user_number += 1

        elif kind == 'src_follower':
            # src_follower part
            self.read_src_from_db()
            src_user_number = 0
            # for all src's
            while src_user_number < len(self.src_user_uid):
                self.continue_flag = 1
                url_page = 0
                self.url_uid = self.src_user_uid[src_user_number]
                self.src_follower_url_head = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_%s&luicode=10000011&lfid=100505%s&featurecode=20000320&type=uid&value=%s&since_id=' % (
                    self.url_uid, self.url_uid, self.url_uid)
                # each src, for all followers
                while self.continue_flag:
                    url_page += 1
                    url = self.src_follower_url_head + str(url_page)
                    yield scrapy.Request(url=url, callback=self.parse_user_info)
                    if not self.continue_flag:
                        break
                src_user_number += 1



        elif kind == 'tweets':
            # tweets part
            # self.read_src_from_db()
            self.read_followed_from_db()
            self.src_user_uid.extend(self.followed_by_src_uid)
            # self.read_follower_from_db()
            # self.src_user_uid.extend(self.src_follower_uid)
            # now src_user_uid contains both src's and follow's, and we re about to crawl their tweets
            # self.src_user_uid.extend(self.followed_by_src_uid)
            src_user_number = 25460
            # for all src's
            while src_user_number < len(self.src_user_uid):
                self.continue_flag = 1
                url_page = 0
                self.url_uid = self.src_user_uid[src_user_number]
                url_head = 'https://m.weibo.cn/api/container/getIndex?type=uid&value=%s&containerid=107603%s&page=' % (
                    self.url_uid, self.url_uid)
                # each src, for all followers
                while self.continue_flag:
                    url_page += 1
                    url = url_head + str(url_page)
                    yield scrapy.Request(url=url, callback=self.parse_tweets)
                    if not self.continue_flag:
                        break
                src_user_number += 1
                print ("***********user_number:%d***************" % src_user_number)

        pass
