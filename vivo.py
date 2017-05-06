# -*- coding: utf-8 -*-
import re
import time
import logging

import scrapy
from scrapy import Request

from items.item import UserItem, PostItem
import dateformatting

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class VivoSpider(scrapy.Spider):
    name = "vivo"
    allowed_domains = ["bbs.vivo.com.cn"]
    urls = (
        {'297': 'http://bbs.vivo.com.cn/forum-107-1.html'},
        {'298': 'http://bbs.vivo.com.cn/forum-113-1.html'},
        {'299': 'http://bbs.vivo.com.cn/forum-118-1.html'},
        {'300': 'http://bbs.vivo.com.cn/forum-121-1.html'},
        {'302': 'http://bbs.vivo.com.cn/forum-140-1.html'},
        {'303': 'http://bbs.vivo.com.cn/forum-141-1.html'},
        {'305': 'http://bbs.vivo.com.cn/forum-151-1.html'},
        {'306': 'http://bbs.vivo.com.cn/forum-152-1.html'},
        {'307': 'http://bbs.vivo.com.cn/forum-164-1.html'},
        {'308': 'http://bbs.vivo.com.cn/forum-165-1.html'},
        {'309': 'http://bbs.vivo.com.cn/forum-6-1.html'},
        {'310': 'http://bbs.vivo.com.cn/forum-77-1.html'},
    )
    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "USER_AGENT": "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
        "DEPTH_LIMIT": 1000
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        settings = crawler.settings
        day = settings["DAY"]
        spider = cls(day)
        spider._set_crawler(crawler)
        return spider

    def __init__(self, day):
        super(VivoSpider, self).__init__()
        self._day = int(str(day).strip())
        logger.info("day: %s" % self._day)

    def start_requests(self):
        for url in self.urls:
            for key, value in url.items():
                yield Request(value, callback=self.parse, meta={'entry_id': key})

    def parse(self, response):
        entry_id = response.meta['entry_id']
        # #匹配分页
        dest_time = response.xpath('//div[@class="by poster"]/em/a/text()').extract()
        dest_time = ''.join(dest_time[-1]).encode('utf-8')
        dest_time = dateformatting.parse(dest_time).strftime('%Y-%m-%d %H:%M:%S')
        flag = self.cal_time(dest_time)
        if flag:
            logger.info('抓取的帖子时间：%s' % dest_time)
            page_urls = response.xpath('//span[@id="fd_page_bottom"]//a[@class="nxt"]/@href').extract()
            page_url = response.urljoin(page_urls[0])
            yield Request(page_url, callback=self.parse, meta={'entry_id': entry_id})
        # 匹配详情页
        details = response.xpath('//tr//a[@class="s xst"]/@href').extract()
        for detail in details:
            detail_url = response.urljoin(detail)
            yield Request(detail_url, callback=self.parse_content, meta={'parent_url': detail_url, 'entry_id': entry_id})
        # 匹配user
        user_urls = response.xpath('//div[@class="by author"]/cite/a/@href').extract()
        for usr in user_urls:
            usr = response.urljoin(usr)
            yield Request(usr, callback=self.parse_user, meta={'entry_id': entry_id})

    def parse_user(self, response):
        '''
        匹配用户基本信息,匹配准确.
        在parse_user_next匹配不稳定,但信息更详细
        '''
        user = UserItem()
        author_id = response.xpath('//div[@class="pbm mbm bbda cl"]//h2[@class="mbn"]//span[@class="xw0"]/text()').extract()[0].encode('utf-8')
        author_id = ''.join(re.findall(r'\d+', author_id))
        author_name = response.xpath('//div[@class="h cl"]//h2[@class="mt"]/text()').extract()[0]
        avatar = ''.join(response.xpath('//div[@class="h cl"]/div[@class="icn avt"]//img/@src').extract())
        home_page = response.url
        follow_num = ''.join(response.xpath('//ul[@class="cl bbda pbm mbm"]/li/a[1]/text()').extract()).encode('utf-8')
        follow_num = ''.join(re.findall(r'\d+', follow_num))
        level = ''.join(response.xpath('//div[@class="bm_c"]//font/text()').extract())
        exp = ''.join(response.xpath('//div[@id="psts"]//ul/li[2]/text()').extract())
        entry_id = response.meta['entry_id']
        register_time = ''.join(response.xpath('//ul[@id="pbbs"]/li[2]/text()').extract())
        post_num = response.xpath('//ul[@class="cl bbda pbm mbm"]/li/a[5]/text()').extract()[0].encode('utf-8')
        post_num = re.findall(r'\d+', post_num)[0]
        comment_num = response.xpath('//ul[@class="cl bbda pbm mbm"]/li/a[6]/text()').extract()[0].encode('utf-8')
        comment_num = re.findall(r'\d+', comment_num)[0]
        photo_num = response.xpath('//ul[@class="cl bbda pbm mbm"]/li/a[4]/text()').extract()[0].encode('utf-8')
        photo_num = re.findall(r'\d+', photo_num)[0]

        user['author_id'] = author_id
        user['author_name'] = author_name
        user['avatar'] = avatar
        user['home_page'] = home_page
        user['follow_num'] = follow_num
        user['level'] = level
        user['exp'] = exp
        user['entry_id'] = entry_id
        user['register_time'] = register_time
        user['post_num'] = post_num
        user['comment_num'] = comment_num
        user['photo_num'] = photo_num
        logger.info('user:%s' % user)
        yield user

    def parse_content(self, response):
        items1 = response.xpath('//div[@id="postlist"]/div[1]')
        items = response.xpath('//div[@id="comment_list"]/div')
        if items != []:
            items.pop()
        items = items1 + items
        parent_url = response.meta['parent_url']
        for item in items:
            # 发表时间
            post = PostItem()
            post_time = item.xpath('.//div[@class="authi"]//span/@title').extract()
            if post_time == []:
                post_time = item.xpath('.//div[@class="authi"]/em/text()').extract()
                if post_time != []:
                    post_time = post_time[0].split(' ')
                    if len(post_time) == 3:
                        post_time.pop(0)
                    post_time = ' '.join(post_time).encode('utf-8')
            if post_time == '':
                continue
            post_time = dateformatting.parse(post_time).strftime('%Y-%m-%d %H:%M:%S')
            site_type = 2
            target = ''.join(item.xpath('.//td[@class="plc plcon"]//strong/a//text()').extract())
            url = re.findall(ur"\d+[\u4e00-\u9fa5]+", target)
            if url == []:
                url = re.findall(ur"[\u4e00-\u9fa5]+", target)
            url = response.url + '#' + ''.join(url).encode('utf-8')
            author_name = item.xpath('.//div[@class="authi"]/a[@class="xw1"]/text()').extract()
            if author_name != []:
                author_name = author_name[0].encode('utf-8')
            text = item.xpath('.//td[@class="t_f"]//text()').extract()
            text = ''.join(text).encode('utf-8')
            img_url = item.xpath('.//td[@class="t_f"]//img/@zoomfile').extract()
            img_list = []
            for ur in img_url:
                ur = response.urljoin(ur)
                img_list.append(ur)
            img_url = img_list

            text = ''.join(text)
            post['post_time'] = post_time
            post['site_type'] = site_type
            post['author_name'] = author_name
            x = ''.join(item.xpath('.//a[@class="show"]/text()').extract()).encode('utf-8')
            if '阅读模式' in x:
                title = response.xpath('//h1[@class="ts"]/a/text()').extract()[0]
                read_num = response.xpath('//div[@class="authi"]//span[@class="xi1 views"][1]/text()').extract()[0]
                comment_num = response.xpath('//div[@class="authi"]//span[@class="xi1 replies"]/text()').extract()[0]
                post['url'] = parent_url
                post['title'] = title
                post['read_num'] = read_num
                post['comment_num'] = comment_num
                post['data_type'] = 'first'
            else:
                post['url'] = url
                post['data_type'] = 'comment'
                post['parent_url'] = parent_url
            post['text'] = text
            post['img_url'] = img_url
            post['entry_id'] = response.meta['entry_id']
            post['include_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            uid = item.xpath('.//a[@class="xw1"]/@href').extract()
            uid = ''.join(re.findall(r'uid-(\d+)', ''.join(uid)))
            post['author_id'] = uid
            logger.info('post:%s' % post)
            yield post
        # 匹配detail分页
        detail_urls = response.xpath('//div[@class="pgs mtm mbm cl"]/div[@class="pg"]/a/@href').extract()
        for detail_url in detail_urls:
            yield Request(response.urljoin(detail_url), callback=self.parse_content,
                          meta={'parent_url': parent_url, 'entry_id': response.meta['entry_id']})

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
