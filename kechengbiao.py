# -*- coding: utf-8 -*-

import json
import time

import logging
import scrapy
from dateutil import parser
from scrapy import Spider
from scrapy import Request

import dateformatting

from items.item import UserItem, PostItem

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class KechengbiaoSpider(Spider):
    '''超级课程表'''
    name = "kechengbiao"
    login_url = 'http://120.55.151.61/V2/StudentSkip/loginCheckV4.action'
    cookie = {}
    user_data = {'platform': '1', 'versionNumber': '8.0.1', 'phoneModel': '2014812', 'channel': '360Market',
                 'phoneVersion': '22', 'phoneBrand': 'xiaomi', 'password': 'AF18A20E1C8B3ABFCD4BA5CD826C288D',
                 'account': '8539A2612F7AF5018B2111F26D85D996',
                 'deviceCode': '867614021039359'}
    content_url = 'http://120.55.151.61/V2/Treehole/Message/getMessageListByFollowTopic.action'
    # content_data = {'platform': '1', 'timestamp': '1488765898270', 'versionNumber': '8.0.1',
    #                 'phoneModel': '2014812', 'channel': '360Marcket', 'phoneVersion': '22', 'phoneBrand': 'xiaomi'}
    comments_data = 'platform=1&phoneVersion=22&phoneBrand=Xiaomi&versionNumber=8.0.1&phoneModel=2014812\
    &channel=360Market&plateId=0&messageId={}&'
    comments_url = 'http://120.55.151.61/Treehole/V4/Message/getDetail.action'
    data = 'platform=1&timestamp={}&versionNumber=8.0.1&\
    phoneModel=2014812&channel=360Market&phoneVersion=22&phoneBrand=Xiaomi&'
    user_body = 'platform=1&versionNumber=8.0.1&phoneModel=2014812&studentId={}&channel=360Market\
    &phoneVersion=22&phoneBrand=Xiaomi&'
    custom_settings = {"DOWNLOAD_DELAY": 2}
    head = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': '120.55.151.61', 'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 5.1.1; 2014812 Build/LMY49J)'}
    user_url = 'http://120.55.151.61/V2/Student/getInfoByIdV4.action'
    count = 0
    gender = {
        '0': 'f',
        '1': 'm',
        '5': 'n',
    }
    data_li = (
        {'50078': '热门影视搜罗'},
        {'50079': '读书使我快乐'},
        {'50080': '北京单身狗的日常'},
        {'50081': '我这么美我不能死'},
        {'50082': '一起来运动'},
        {'50083': '星座那些事儿'},
        {'50084': '同性恋又怎么了'},
        {'50085': '表情包前线战区'},
        {'50086': '我的爱豆日常'},
        {'50087': '我要当麦霸'},
        {'50088': '考研天地'},
        {'50089': '我是学霸我骄傲'},
        {'50090': '关注就会有男友'},
    )

    def start_requests(self):
        # yield Request(self.content_url, self.parse_content, method="POST",
        #               body='platform=1&timestamp=1488765898270&versionNumber=8.0.1
        # &phoneModel=2014812&channel=360Market&phoneVersion=22&phoneBrand=Xiaomi&',
        #               headers=self.head, cookies={'JSESSIONID': '05AF9BD569FA4F95506D71D386D1F686-memcached1'})
        yield scrapy.FormRequest(self.login_url, formdata=self.user_data, callback=self.parse_cookie)

    def parse_cookie(self, response):
        c = dict(response.headers)['Set-Cookie'][0].split(';')[0].split('=')
        self.cookie[c[0]] = c[1]
        # logger.info(self.cookie)
        yield Request(self.content_url, self.parse_content, method='Post',
                      body=self.data.format(0), headers=self.head, cookies=self.cookie)

    def parse_user(self, response):
        data = json.loads(response.body)['data']
        author_id = response.meta['uid']
        user = UserItem()
        user['author_id'] = author_id
        user['author_name'] = data['nickName']
        user['avatar'] = data['avatarUrl']
        user['home_page'] = 'http://120.55.151.61/'
        user['post_num'] = data.get('realNameMsgNum', 0)
        user['city'] = data.get('bornCity', '')
        user['province'] = data.get('bornProvince', '')
        user['education'] = data.get('schoolName', '')
        user['gender'] = self.gender.get(str(data.get('gender', 5)), 'n')
        logger.info('gender:%s' % data.get('gender', ''))
        birthday = data.get('bornDate', 0)
        if birthday != 0:
            birthday = int(str(birthday)[:-3])
        else:
            birthday = 0
        user['birthday'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(birthday))
        user['description'] = data.get('signature', '')
        logger.info('user:%s' % user)
        yield user

    def parse_content(self, response):
        data = json.loads(response.body)
        for post in data['data']['messageBOs']:
            author_id = post.get('studentBO', {}).get('id', 0)
            yield Request(self.user_url, method='Post', body=self.user_body.format(author_id), meta={'uid': author_id},
                          headers=self.head, cookies=self.cookie, callback=self.parse_user)
            self.count += 1
            postitem = PostItem()
            pid = post['messageId']
            postitem['author_id'] = post.get('tudentBO', {}).get('id', 0)
            postitem['title'] = post.get('treeholeSimpleBO', {}).get('nameStr', 0)
            create_at = self.formate_date(time.strftime('%Y-%m-%d %H:%M:%S',
                                                        time.localtime(int(str(post['issueTime'])[0: -3]))))
            postitem['text'] = post['content']
            postitem['include_time'] = create_at
            postitem['comment_num'] = post['comments']
            postitem['like_num'] = post['likeCount']
            img = post.get('qiniuImgBOs', [])
            if img != []:
                postitem['img_url'] = img[0].get('url', '#')
                logger.info(img[0].get('url', '#'))
            # postitem['app_name'] = self.name
            postitem['read_num'] = post['readCount']
            # postitem[''] = {'schoolName': post['schoolName'], 'schoolId': post['schoolId']}
            # logger.info(postitem)
            # yield Request(self.comments_url, self.parse_comments, method='Post', meta={'postitem': postitem},
            #               body=self.comments_data.format(pid), headers=self.head, cookies=self.cookie)
            # logger.info(postitem['content'])
        timestamp = data['data']['timestampLong']

        yield Request(self.content_url, self.parse_content, method='Post', body=self.data.format(timestamp),
                      headers=self.head, cookies=self.cookie)

    def parse_comments(self, response):
        postitem = response.meta['postitem']
        data = json.loads(response.body)['data']
        li = []
        for comment in data['commentListBO']['commentBOs']:
            com = '#' + str(comment['floor']) + comment['content']
            li.append(com)
        comments = ''.join(li)
        postitem['text'] = postitem['text'] + comments
        # logger.info(postitem['content'])
        logger.info(dict(postitem))
        yield postitem

    def formate_date(self, datestr):
        """formate date like:
            2017-03-01T09:19:56.587+08:00
        """
        return str(parser.parse(datestr)).split('+')[0]
