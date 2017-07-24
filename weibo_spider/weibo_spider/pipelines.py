# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
from items import userinfoItem, tweetItem
import time

db_user = 'industry'
db_pass = '123456industry'
db_host = '192.168.2.6'
db_port = '3306'
db_name = 'dingfu_industry'
db_default_character_set = 'utf8mb4'

batch_page_max = 1


class WeiboSpiderPipeline(object):#using df industry as db
    def __init__(self):
        self.connect = MySQLdb.connect(user=db_user, passwd=db_pass, db=db_name, host=db_host, charset="utf8",  use_unicode=True)
        self.cursor = self.connect.cursor()
        # Enforce UTF-8 for the connection.
        self.cursor.execute('SET NAMES utf8mb4')
        self.cursor.execute("SET CHARACTER SET utf8mb4")
        self.cursor.execute("SET character_set_connection=utf8mb4")
        self.batched_pages = 0
        self.s=[]
        self.s_edges=[]
        fp = open("edges.log", "wb+")
        fp.close()

    def process_item(self, item, spider):
        start_time=time.time()
        response = item['response']
        src = response[u'cardlistInfo'][u'containerid'].split('_')[-1]
        content = response[u'cards']
        self.batched_pages += 1
        # user info part
        if isinstance(item, userinfoItem):
            users = content[-1][u'card_group']
            sql = "INSERT INTO df_weibo_%s_test " % item['kind']
            sql += "(uid,screen_name,gender,verified_reason,follow_cnt, followers_cnt, statuses_cnt) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE screen_name=VALUES(screen_name), verified_reason=VALUES(verified_reason) ,follow_cnt=VALUES(follow_cnt), followers_cnt=VALUES(followers_cnt), statuses_cnt=VALUES(statuses_cnt)"
            i=0
            while i < len(users):

                user = users[i][u'user']
                try:
                    uid = str(user[u'id'])
                    screen_name = user[u'screen_name']
                    gender = user[u'gender']
                    follow_cnt = user[u'follow_count']
                    followers_cnt = user[u'followers_count']
                    status_cnt = user[u'statuses_count']
                    if user[u'verified']:
                        verified_reason = user[u'verified_reason']
                    else:
                        verified_reason = '-1'
                    self.s.append([uid.encode('utf-8'), screen_name.encode('utf-8'), gender.encode('utf-8'),
                              verified_reason.encode('utf-8'), follow_cnt, followers_cnt, status_cnt])
                    #src follower
                    edge=uid+'-'+src+'\n'
                    #followed by src
                    #edge=src+'-'+uid+'\n'
                    self.s_edges.append(edge.encode('utf-8'))
                except:
                    pass

                i += 1
            pass


        #tweet part
        elif isinstance(item, tweetItem):
            sql = 'INSERT INTO df_weibo_tweets (uid,screen_name,tweet_id,created_at,text,url,likes,comments_cnt,retweeted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE  screen_name=VALUES(screen_name), likes=VALUES(likes), comments_cnt=VALUES(comments_cnt)'
            i = 0
            while i < len(content):
                if u'mblog' not in content[i]:
                    i += 1
                    continue
                tweet = content[i][u'mblog']
                tweet_id = tweet[u'idstr']
                url = str(content[i][u'scheme'])
                user = tweet[u'user']
                creat_time = tweet[u'created_at']

                if u'今天' in creat_time:
                    creat_time = time.strftime('%Y-%m-%d', time.localtime(time.time())) + creat_time.strip(u'今天')
                    pass
                elif u'前' in creat_time:
                    creat_time = time.strftime('%Y-%m-%d %H:%M',
                                               time.localtime(time.time() - 60 * int(creat_time.strip(u'分钟前'))))
                    # self.continue_flag = 0
                    # return
                    pass
                elif len(creat_time) <= 12:
                    creat_time = time.strftime('%Y-', time.localtime(time.time())) + creat_time
                    pass

                try:
                    uid = str(user[u'id'])
                    screen_name = user[u'screen_name']
                    tweet_id = tweet_id
                    text = tweet[u'text'].replace('\'','')
                    url = url.replace('\'','')
                    comments_cnt = tweet[u'comments_count']
                    likes = tweet[u'attitudes_count']
                    created_at = creat_time
                    if u'retweeted_status' in tweet:
                        retweeted = tweet[u'retweeted_status'][u'text'].replace('\'','')
                    else:
                        retweeted = '-1'
                    self.s.append([uid.encode('utf-8'), screen_name.encode('utf-8'), tweet_id.encode('utf-8'),
                              created_at.encode('utf-8'), text.encode('utf-8'), url.encode('utf-8'),
                              likes, comments_cnt, retweeted.encode('utf-8')])
                except:
                    pass

                i += 1
            pass


        if self.batched_pages > batch_page_max:
            print "***********writing %d pages into MYSQL***********" % batch_page_max
            try:
                self.cursor.executemany(sql, self.s)
                self.connect.commit()
            except MySQLdb.Error, e:
                print "Error %d: %s" % (e.args[0], e.args[1])
            if len(self.s_edges):
                print "***********writing %d pages into txt***********" % batch_page_max
                try:
                    fp=open("edges.log","ab+")
                    fp.writelines(self.s_edges)
                    fp.close()
                finally:
                    pass
            self.batched_pages = 0
            self.s=[]
            self.s_edges=[]
            pass

        end_time=time.time()
        delta=end_time-start_time
        #print delta
        return

        #return item