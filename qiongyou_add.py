# -*- coding: utf-8 -*-
import re
import time
import logging

import scrapy
from scrapy import Request
import json

from items.item import UserItem, PostItem
import dateformatting

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class QiongyouSpider(scrapy.Spider):
    name = "qiongyou"
    urls = (
        {"http://ask.qyer.com/news": "50076"},           # 问答

    )
    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "USER_AGENT": "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
        "DEPTH_LIMIT": 1000
    }
    user_url = 'http://www.qyer.com/u/{}/profile'
    wenda_url = 'http://ask.qyer.com/api/index/news'
    page_num = 1

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        settings = crawler.settings
        day = settings["DAY"]
        spider = cls(day)
        spider._set_crawler(crawler)
        return spider

    def __init__(self, day):
        super(QiongyouSpider, self).__init__()
        self._day = int(str(day).strip())
        logger.info("day: %s" % self._day)

    def start_requests(self):
        data = {'page': '1'}
        yield scrapy.FormRequest(url=self.wenda_url, callback=self.parse, formdata=data,
                                 meta={'entry_id': 50076})

    def parse(self, response):
        logger.info(response.url)
        entry_id = response.meta['entry_id']
        data = json.loads(response.body)['data']
        for it in data:
            detail_url = response.urljoin(it['question_url'])
            post = PostItem()
            post_time = dateformatting.parse(it['question_date']).strftime('%Y-%m-%d %H:%M:%S')
            post['author_id'] = it['question_uid']
            post['url'] = detail_url
            post['title'] = it['question_title']
            post['comment_num'] = it['question_renum']
            post['data_type'] = 'first'
            post['post_time'] = post_time
            post['site_type'] = 15
            post['author_name'] = it['question_username']
            post['text'] = it['question_content']
            # post['img_url'] = img_url
            post['entry_id'] = entry_id
            post['include_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            yield post
            logger.info('post_louzhu:%s' % post)
            yield Request(detail_url, callback=self.parse_content, meta={'parent_url': detail_url, 'entry_id': entry_id})
        # #匹配分页
        dest_time = data[-1]['question_date']
        dest_time = dateformatting.parse(dest_time).strftime('%Y-%m-%d %H:%M:%S')
        flag = self.cal_time(dest_time)
        if flag:
            logger.info('抓取的帖子时间：%s' % dest_time)
            self.page_num += 1
            dat = {'page': str(self.page_num)}
            yield scrapy.FormRequest(url=self.wenda_url, callback=self.parse, formdata=dat,
                                     meta={'entry_id': entry_id})

    def parse_user(self, response):
        entry_id = response.meta['entry_id']
        uid = response.meta['uid']
        user = UserItem()
        author_id = uid
        author_name = response.meta['author_name']
        avatar = ''.join(response.xpath('//div[@class="face"]//img/@src').extract())
        avatar = response.urljoin(avatar)
        follow_num = response.xpath('//li[@data-bn-ipg="usercenter-setting-follow"]//text()').extract()
        # if follow_num == []:
        #     follow_num = response.xpath('//ul[@class="cl bbda pbm mbm"]/li[1]/a[1]/text()').extract()
        follow_num = ''.join(re.findall(r'\d+', ''.join(follow_num)))
        fans_num = response.xpath('//li[@data-bn-ipg="usercenter-setting-fan"]//text()').extract()
        fans_num = ''.join(re.findall(r'\d+', ''.join(fans_num)))
        level = ''.join(response.xpath('//a[@data-bn-ipg="usercenter-grade"]//text()').extract())
        gender = ''.join(response.xpath('//ul[@class="clearfix fontArial"]/li[2]//text()').extract()).encode('utf-8')
        city = ''.join(response.xpath('//li[@data-bn-ipg="usercenter-setprofile-living"]/div[@class="right"]/text()').extract())
        if '女' in gender:
            gender = 'f'
        elif '男' in gender:
            gender = 'm'
        else:
            gender = 'n'
        user['gender'] = gender
        user['level'] = level
        user['author_id'] = author_id
        user['follow_num'] = follow_num
        user['author_name'] = author_name
        user['avatar'] = avatar
        user['home_page'] = response.url
        user['entry_id'] = entry_id
        user['fans_num'] = fans_num
        user['city'] = city
        # user['register_time'] = register_time
        # user['post_num'] = post_num
        # user['comment_num'] = comment_num
        logger.info('user:%s' % user)
        yield user

    def parse_content(self, response):
        entry_id = response.meta['entry_id']
        items = response.xpath('//div[@class="mod_discuss_box"]')
        parent_url = response.meta['parent_url']
        for item in items:
            # 发表时间
            post = PostItem()
            post_time = ''.join(item.xpath('.//span[@class="answer_time"]/a/text()').extract()).encode('utf-8')
            logger.info(post_time)
            post_time = dateformatting.parse(post_time).strftime('%Y-%m-%d %H:%M:%S')
            site_type = 15
            # logger.info(post_time)
            url = response.url + '#' + post_time
            author_name = item.xpath('.//div[@class="mod_discuss_box_name"]/a/text()').extract()
            author_name = ''.join(author_name).encode('utf-8')
            text = item.xpath('.//div[@class="mod_discuss_box_text qyer_spam_text_filter"]//text()').extract()
            text = ''.join(text).encode('utf-8')
            img_url = item.xpath('.//div[@class="mod_discuss_box_text qyer_spam_text_filter"]//img/@data-original').extract()
            if img_url == []:
                img_url = item.xpath('.//ul[@class="xpc"]//img/@data-original').extract()
            li = []
            if img_url != []:
                for img in img_url:
                    img = response.urljoin(img)
                    li.append(img)
                img_url = li
            if img_url == []:
                img_url = ''
            post['post_time'] = post_time
            post['site_type'] = site_type
            post['author_name'] = author_name
            post['url'] = url
            post['data_type'] = 'comment'
            post['parent_url'] = parent_url
            post['text'] = text
            post['img_url'] = img_url
            post['entry_id'] = entry_id
            post['include_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            # 匹配user
            uid = item.xpath('.//div[@class="mod_discuss_box_name"]/a/@href').extract()
            uid = ''.join(re.findall(r'/u/(\d+)', uid[0]))
            post['author_id'] = uid
            logger.info('post:%s' % post)
            yield post
            # 匹配user
            yield Request(self.user_url.format(uid), callback=self.parse_user,
                          meta={"entry_id": entry_id, 'uid': uid, 'author_name': author_name})
            # # 匹配detail分页
        louzhu_url = ''.join(response.xpath('//a[@class="avatar"]/@href').extract())
        uid = ''.join(re.findall(r'/u/(\d+)', louzhu_url))
        author_name = ''.join(response.xpath('//div[@class="question-info clearfix mt10"]/a/text()').extract())
        yield Request(self.user_url.format(uid), callback=self.parse_user,
                      meta={"entry_id": entry_id, 'uid': uid, 'author_name': author_name})

    def cal_time(self, dest_time):
        time_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        time_now_day = int(''.join(re.findall(r'\d+-\d+-(\d+)', time_now)))
        time_dest_day = int(''.join(re.findall(r'\d+-\d+-(\d+)', dest_time)))
        year_now = int(''.join(re.findall(r'(\d+)-\d+-\d+', time_now)))
        year_dest = int(''.join(re.findall(r'(\d+)-\d+-\d+', dest_time)))
        month_now = int(''.join(re.findall(r'\d+-(\d+)-\d+', time_now)))
        month_dest = int(''.join(re.findall(r'\d+-(\d+)-\d+', dest_time)))
        day_now = 365 * year_now + 30 * month_now + time_now_day
        day_dest = 365 * year_dest + 30 * month_dest + time_dest_day
        if day_now - day_dest > self._day or day_now - day_dest < 0:
            flag = False
            return flag
        else:
            flag = True
            return flag
