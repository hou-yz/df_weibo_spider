# -*- coding: utf-8 -*-

from scrapy import signals
import requests
import random
import time

from scrapy.utils.response import response_status_message


user_agent = [
    'Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.114 Mobile Safari/537.36',
    'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
    'Mozilla/5.0 (Linux; Android 4.4.2; NX505J Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0 Mobile Safari/537.36 Weibo (nubia-NX505J__weibo__6.8.0__android__android4.4.2) tae_sdk_a_2.0 AliApp(BC/2.0)',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G34 Weibo (iPhone8,1__weibo__6.8.1__iphone__os9.3.3) AliApp(BC/2.1) tae_sdk_ios_2.1 havana',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)',
    'Mozilla/5.0 (Linux; U; Android 4.0.1; ja-jp; Galaxy Nexus Build/ITL41D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
    'Mozilla/5.0 (Linux; U; Android 4.0.3; ja-jp; URBANO PROGRESSO Build/010.0.3000) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
    'Mozilla/5.0 (Linux; U; Android 4.0.3; ja-jp; Sony Tablet S Build/TISU0R0110) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30',
    'Mozilla/5.0 (Linux; U; Android 4.0.4; ja-jp; SC-06D Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
    'Mozilla/5.0 (Linux; U; Android 4.1.1; ja-jp; Galaxy Nexus Build/JRO03H) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
    'Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03S) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19',
    'Mozilla/5.0 (Linux; U; Android 4.4.2; zh-CN; NX505J Build/KVT49L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.5.2.598 U3/0.8.0 Mobile Safari/534.30',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3 like Mac OS X; zh-CN) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/13G34 UCBrowser/10.9.19.815 Mobile',
    'Mozilla/5.0 (Linux; Android 4.4.2; NX505J Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/35.0.1916.138 Mobile Safari/537.36 T7/7.1 baiduboxapp/7.5.1 (Baidu; P1 4.4.2)',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G34 baiduboxapp/0_7.0.5.7_enohpi_4331_057/3.3.9_1C2%258enohPi/1099a/B80ADBCDA918AA94450DE8767C97BC446249D6087FCLPRHEEPF/1',
    'Mozilla/5.0 (Linux; Android 4.4.2; NX505J Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/35.0.1916.138 Mobile Safari/537.36 T7/7.2 baidubrowser/7.3.14.0 (Baidu; P1 4.4.2)',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.3 Mobile/13G34 Safari/600.1.4 baidubrowser/4.2.0.324 (Baidu; P2 9.3.3)',
    'Mozilla/5.0 (Linux; Android 4.4.2; NX505J Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile MQQBrowser/6.2 TBS/036555 Safari/537.36 V1_AND_SQ_6.3.7_374_YYB_D PA QQ/6.3.7.2795 NetType/WIFI WebP/0.3.0 Pixel/1080',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G34 QQ/6.5.0.443 V1_IPH_SQ_6.5.0_1_APP_A Pixel/750 Core/UIWebView NetType/WIFI Mem/41',
    'Mozilla/5.0 (Linux; Android 4.4.2; NX505J Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile MQQBrowser/6.2 TBS/036555 Safari/537.36 V1_AND_SQ_6.3.7_374_YYB_D QQ/6.3.7.2795 NetType/WIFI WebP/0.3.0 Pixel/1080',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G34 QQ/6.5.0.443 V1_IPH_SQ_6.5.0_1_APP_A Pixel/750 Core/UIWebView NetType/WIFI Mem/169',
    'Mozilla/5.0 (Linux; U; Android 4.4.2; zh-cn; NX505J Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/37.0.0.0 MQQBrowser/6.8 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone 6s; CPU iPhone OS 9_3_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/6.0 MQQBrowser/6.8.1 Mobile/13G34 Safari/8536.25 MttCustomUA/2',
    'Mozilla/5.0 (Linux; Android 4.4.2; NX505J Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile MQQBrowser/6.2 TBS/036548 Safari/537.36 MicroMessenger/6.3.18.800 NetType/WIFI Language/zh_CN',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G34 MicroMessenger/6.3.23 NetType/WIFI Language/zh_CN',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0)',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.2)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)',
    'Mozilla/4.0 (compatible; MSIE 5.0; Windows NT)',
    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11',
    'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11',
    ]
timeout_error_max = len(user_agent)*0.3
continuous_error_max = 4
proxy_pool_length = 8
proxy_lastminute_min_url_length = 500000


class ProxyMiddleware(object):
    proxy_pool = []
    proxy_ua_continuous_error = []
    proxy_start_time = []
    proxy_lastminute = []
    proxy_lastminute_url_length = []
    proxy_allowed = []
    outdated_pool = []


    def __init__(self):
        t=requests.get('http://http.zhimadaili.com/getip?num=%s&type=1&pro=&city=0&yys=0&port=1&time=2' % (str(proxy_pool_length))).content
        t=t.split('<br/>\r\n')
        del t[len(t)-1]
        fp = open('info.log', 'wb+')
        fp.write('********************************************************************\n')
        for line in t:
            fp.write('%s\tnew\t\t%s\n' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),line))
            self.proxy_ua_continuous_error.append(list(0 for i in range(0,len(user_agent))))
            self.proxy_lastminute_url_length.append(0)
            self.proxy_start_time.append(time.time())
            self.proxy_lastminute.append(time.time())
            self.proxy_allowed.append(1)
        self.proxy_pool.extend(t)

        fp.write('********************************************************************\n')
        fp.close()
        pass

    def process_request(self, request, spider):
        start_time = time.time()
        for proxy_index in range(0,len(self.proxy_pool)):
            if start_time - self.proxy_lastminute[proxy_index] > 60:
                self.proxy_lastminute[proxy_index] = start_time
                self.proxy_lastminute_url_length[proxy_index] = 0
                self.proxy_allowed[proxy_index] = 1
                self.proxy_ua_continuous_error[proxy_index] = list(0 for i in range(0,len(user_agent)))
        pass
        allowed_proxy_index = [i for i in range(0, len(self.proxy_pool)) if (self.proxy_allowed[i] and len([j for j in range(0,len(user_agent)) if self.proxy_ua_continuous_error[i][j]>=0 and self.proxy_ua_continuous_error[i][j]<continuous_error_max]))]
        if len(allowed_proxy_index)==0:
            print "********WARNING: IP BANNED, PAUSE*********"
            sleeptime=60-start_time+max(self.proxy_lastminute)
            print "sleep:%d" % sleeptime
            time.sleep(sleeptime)
            return self.retry_request(request)
        proxy_index = random.sample(allowed_proxy_index, 1)[0]
        proxy=self.proxy_pool[proxy_index]
        request.meta['proxy'] = 'https://'+proxy
        allowed_ua_index = [i for i in range(0,len(user_agent)) if self.proxy_ua_continuous_error[proxy_index][i]>=0 and self.proxy_ua_continuous_error[proxy_index][i]<continuous_error_max]
        ua_index = random.sample(allowed_ua_index,1)[0]
        request.headers.setdefault('User-Agent', user_agent[ua_index])
        pass

    def process_response(self, request, response, spider):
        start_time = time.time()
        last_proxy = request.meta['proxy'].strip('https://')
        last_ua = request.headers['User-Agent']
        if last_proxy in self.proxy_pool:
            proxy_index = self.proxy_pool.index(last_proxy)
            ua_index = user_agent.index(last_ua)
            self.proxy_lastminute_url_length[proxy_index] += len(response.body)
            last_page=int(response._url.split('=')[-1])

            if response.status >= 400:
                if response.status == 414:
                    if self.proxy_allowed[proxy_index]:
                        print "***********414 at ip%s, url length=%d**************" % (last_proxy,self.proxy_lastminute_url_length[proxy_index])
                        self.proxy_allowed[proxy_index] = 0
                    #self.update_proxy(u'IP_BANNED', request, response_status_message(response.status))
                if self.proxy_ua_continuous_error[proxy_index][ua_index] != -1:
                    self.proxy_ua_continuous_error[proxy_index][ua_index] += 1
                if len(list(1 for i in self.proxy_ua_continuous_error[proxy_index] if i >= continuous_error_max)) >= len(user_agent)*0.8:
                    #self.update_proxy(u'HTTP_ERROR', request, response_status_message(response.status))
                    if self.proxy_allowed[proxy_index]:
                        print "***********ua banned at ip%s, url length=%d**************" % (last_proxy,self.proxy_lastminute_url_length[proxy_index])
                        self.proxy_allowed[proxy_index] = 0
                    pass

                end_time = time.time()
                delta = end_time - start_time
                #print delta
                return self.retry_request(request)

            elif u'-100' in response.body:
                if self.proxy_ua_continuous_error[proxy_index][ua_index] != -1:
                    self.proxy_ua_continuous_error[proxy_index][ua_index] += 1
                if len(list(1 for i in self.proxy_ua_continuous_error[proxy_index] if i >= continuous_error_max)) >= len(user_agent)*0.8:
                    if self.proxy_allowed[proxy_index]:
                        print "***********ip banned at ip%s, url length=%d**************" % (last_proxy,self.proxy_lastminute_url_length[proxy_index])
                        self.proxy_allowed[proxy_index] = 0
                    #self.update_proxy(u'IP_BANNED', request, response_status_message(response.status))

                end_time = time.time()
                delta = end_time - start_time
                #print delta
                return self.retry_request(request)


            else:
                end_time = time.time()
                delta = end_time - start_time
                #print delta
                # success, clear continuous_error
                self.proxy_ua_continuous_error[proxy_index][ua_index] = 0
                return response

    def process_exception(self, request, exception, spider):
        #print exception
        if 'User-Agent'  not in request.headers:
            pass
        start_time = time.time()
        last_proxy = request.meta['proxy'].strip('https://')
        last_ua = request.headers['User-Agent']
        if last_proxy in self.proxy_pool:
            ua_index = user_agent.index(last_ua)
            proxy_index = self.proxy_pool.index(last_proxy)
            if self.proxy_ua_continuous_error[proxy_index][ua_index] != -1:
                self.proxy_ua_continuous_error[proxy_index][ua_index] = -1
            if len(list(1 for i in self.proxy_ua_continuous_error[proxy_index] if i == -1)) >= timeout_error_max \
                    and self.proxy_lastminute_url_length[proxy_index] < proxy_lastminute_min_url_length \
                    and time.time() - self.proxy_start_time[proxy_index] > 20*60:
                self.update_proxy(u'TIMEOUT', request, exception)


        end_time = time.time()
        delta = end_time - start_time
        #print delta
        return self.retry_request(request)

    def update_proxy(self,errortype,request,reason):
        print "**********update proxy************"
        print reason
        last_proxy = request.meta['proxy'].strip('https://')
        last_ua = request.headers['User-Agent']
        proxy_index = self.proxy_pool.index(last_proxy)
        if last_proxy not in self.outdated_pool:
            self.outdated_pool.append(last_proxy)
            fp = open('info.log', 'ab+')
            fp.write(
                "%(time)s [%(error)s]\r\n %(url)s reason: %(reason)s \r\n%(ip)s total url length:%(url_length)s lifetime:%(lifetime)s\r\n" %
                {'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                 'error': errortype,
                 'url': request.url,
                 'reason': reason,
                 'ip': (request.meta['proxy']).strip('\r\n'),
                 'url_length': self.proxy_lastminute_url_length[proxy_index],
                 'lifetime':time.strftime('%H:%M:%S', time.localtime(time.time() - self.proxy_start_time[proxy_index] - 8*3600))})
            fp.write('%s\toutdated:%s' %
                     (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), str(self.outdated_pool.__len__())))
            fp.write('********************************************************************\n')
            fp.close()
        # del and add new proxy
        del self.proxy_pool[proxy_index]
        del self.proxy_lastminute_url_length[proxy_index]
        del self.proxy_ua_continuous_error[proxy_index]
        del self.proxy_lastminute[proxy_index]
        del self.proxy_allowed[proxy_index]
        del self.proxy_start_time[proxy_index]
        while len(self.proxy_pool) < proxy_pool_length:
            #time.sleep(1.1)
            t = requests.get('http://http.zhimadaili.com/getip?num=%s&type=1&pro=&city=0&yys=0&port=1&time=2' % (
                    str(int(proxy_pool_length / 4)))).content
            t = t.split('<br/>\r\n')
            del t[len(t) - 1]
            if t not in self.outdated_pool:
                self.proxy_pool.extend(t)
                for tt in t:
                    self.proxy_ua_continuous_error.append(list(0 for i in range(0,len(user_agent))))
                    self.proxy_lastminute_url_length.append(0)
                    self.proxy_lastminute.append(time.time())
                    self.proxy_start_time.append(time.time())
                    self.proxy_allowed.append(1)

            fp = open('info.log', 'ab+')
            for line in t:
                fp.write('%s\tnew\t\t%s\r\n' % (
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), line))
            fp.close()

            pass


    def retry_request(self,request):
        start_time = time.time()
        for proxy_index in range(0, len(self.proxy_pool)):
            if start_time - self.proxy_lastminute[proxy_index] > 60:
                self.proxy_lastminute[proxy_index] = start_time
                self.proxy_lastminute_url_length[proxy_index] = 0
                self.proxy_allowed[proxy_index] = 1
                self.proxy_ua_continuous_error[proxy_index] = list(0 for i in range(0,len(user_agent)))
        pass
        # retry again.
        last_proxy = request._meta['proxy'].strip('https://')
        last_ua = request.headers['User-Agent']
        allowed_proxy_index = [i for i in range(0, len(self.proxy_pool)) if (self.proxy_allowed[i] and
                               len([j for j in range(0, len(user_agent)) if self.proxy_ua_continuous_error[i][j] >= 0 and self.proxy_ua_continuous_error[i][j] < continuous_error_max]))]


        if len(allowed_proxy_index)==0:
            print "********WARNING: IP BANNED, PAUSE*********"
            sleeptime=60-start_time+max(self.proxy_lastminute)
            print "sleep:%d" % sleeptime
            time.sleep(sleeptime)
            return self.retry_request(request)
        proxy_index = random.sample(allowed_proxy_index, 1)[0]
        proxy=self.proxy_pool[proxy_index]
        if proxy == last_proxy:
            proxy = self.proxy_pool[(proxy_index + 1) % len(self.proxy_pool)]

        allowed_ua_index = [i for i in range(0, len(user_agent)) if (self.proxy_ua_continuous_error[proxy_index][i] >= 0 and self.proxy_ua_continuous_error[proxy_index][i]<continuous_error_max)]
        ua_index = random.sample(allowed_ua_index, 1)[0]
        ua = user_agent[ua_index]


        request.meta['proxy'] = 'https://' + proxy
        request.headers['User-Agent'] = ua

        return request

