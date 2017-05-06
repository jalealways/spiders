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
    allowed_domains = ["www.qyer.com", "bbs.qyer.com"]
    urls = (
        # {"http://bbs.qyer.com/forum-2-1.html": "50039"}, #穷游
        {"http://bbs.qyer.com/forum-3-1.html": "50040"},
        {"http://bbs.qyer.com/forum-22-1.html": "50041"},
        {"http://bbs.qyer.com/forum-88-1.html": "50042"},
        {"http://bbs.qyer.com/forum-122-1.html": "50043"},
        {"http://bbs.qyer.com/forum-49-1.html": "50044"},
        {"http://bbs.qyer.com/forum-100-1.html": "50045"},
        {"http://bbs.qyer.com/forum-14-1.html": "50046"},
        {"http://bbs.qyer.com/forum-12-1.html": "50047"},
        {"http://bbs.qyer.com/forum-16-1.html": "50048"},
        {"http://bbs.qyer.com/forum-15-1.html": "50049"},
        {"http://bbs.qyer.com/forum-162-1.html": "50050"},
        {"http://bbs.qyer.com/forum-25-1.html": "50051"},
        {"http://bbs.qyer.com/forum-13-1.html": "50052"},
        {"http://bbs.qyer.com/forum-52-1.html": "50053"},
        {"http://bbs.qyer.com/forum-57-1.html": "50054"},
        {"http://bbs.qyer.com/forum-106-1.html": "50055"},
        {"http://bbs.qyer.com/forum-164-1.html": "50056"},
        {"http://bbs.qyer.com/forum-165-1.html": "50057"},
        {"http://bbs.qyer.com/forum-163-1.html": "50058"},
        {"http://bbs.qyer.com/forum-108-1.html": "50059"},
        {"http://bbs.qyer.com/forum-175-1.html": "50060"},
        {"http://bbs.qyer.com/forum-104-1.html": "50061"},
        {"http://bbs.qyer.com/forum-177-1.html": "50062"},
        {"http://bbs.qyer.com/forum-59-1.html": "50063"},
        {"http://bbs.qyer.com/forum-103-1.html": "50064"},
        {"http://bbs.qyer.com/forum-54-1.html": "50065"},
        {"http://bbs.qyer.com/forum-53-1.html": "50066"},
        {"http://bbs.qyer.com/forum-168-1.html": "50067"},
        {"http://bbs.qyer.com/forum-55-1.html": "50068"},
        {"http://bbs.qyer.com/forum-56-1.html": "50069"},
        {"http://bbs.qyer.com/forum-83-1.html": "50070"},
        {"http://bbs.qyer.com/forum-178-1.html": "50071"},
        {"http://bbs.qyer.com/forum-173-1.html": "50072"},
        {"http://bbs.qyer.com/forum-174-1.html": "50073"},
        {"http://bbs.qyer.com/forum-86-1.html": "50074"},
        {"http://bbs.qyer.com/forum-60-1.html": "50075"},

    )
    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "USER_AGENT": "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
        "DEPTH_LIMIT": 1000
    }
    user_url = 'http://www.qyer.com/u/{}/profile'
    jieban_url = 'http://bbs.qyer.com/forum-2-1.html'
    jieban_fenye_url = 'http://bbs.qyer.com/thread.php?action=getTogether'
    page_num = 0
    # 要抓取的天数

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
        for url in self.urls:
            for key, value in url.items():
                yield Request(key, callback=self.parse, meta={'entry_id': value})
        yield Request(self.jieban_url, callback=self.parse_jieban, meta={'entry_id': "50039"})

    def parse(self, response):
        logger.info(response.url)
        entry_id = response.meta['entry_id']
        # # 获取社区
        # shequ = response.xpath('//div[@class="q-layer q-layer-section"]//dd/a')
        # for x in shequ:
        #     shequ_name = ''.join(x.xpath('./text()').extract()).encode('utf-8')
        #     shequ_url = response.urljoin(''.join(x.xpath('./@href').extract()))
        #     she = {'name': shequ_name, 'url': shequ_url, 'site_type': 2}
        #     with open('qiongyou.json', 'a') as f:
        #         f.write(json.dumps(she) + '\n')
        # 匹配详情页
        bl = response.xpath('//ul[@id="list-id"]/li')
        for i in bl:
            detail = response.xpath('//a[@class="txt"]/@href').extract_first()
            comment_num = i.xpath('.//span[@class="reply"]//text() | //span[@class="lbvch xnum"]//text()').extract()[0]
            detail_url = response.urljoin(detail)
            yield Request(detail_url, callback=self.parse_content,
                          meta={'parent_url': detail_url, 'entry_id': entry_id, 'comment_num': comment_num})
        # #匹配分页
        dest_time = response.xpath('//span[@class="zdate"]/text()').extract()
        dest_time = ''.join(dest_time[-1]).encode('utf-8').split(' ')[1]
        dest_time = dateformatting.parse(dest_time).strftime('%Y-%m-%d %H:%M:%S')
        flag = self.cal_time(dest_time)
        if flag:
            logger.info('抓取的帖子时间：%s' % dest_time)
            page_urls = response.xpath('//div[@class="ui_page"]/a/@href').extract()
            # for ur in page_urls:
            page_url = response.urljoin(page_urls[-1])
            # logger.info(page_url)
            yield Request(page_url, callback=self.parse, meta={'entry_id': entry_id})

    def parse_jieban(self, response):
        entry_id = response.meta['entry_id']
        dest_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # 匹配结伴同游
        if response.meta.get('fenye'):
            data = json.loads(response.body)['data']
            dest_time = data['res'][-1]['together_info']['departure_time_latest'].replace('.', '-')
            for it in data['res']:
                detail_url = response.urljoin(it['url'])
                yield Request(url=detail_url, callback=self.parse_content,
                              meta={'parent_url': detail_url, 'entry_id': entry_id})
        # 结伴同游分页
        flag = self.cal_time(dest_time)
        if flag:
            logger.info('抓取的帖子时间：%s' % dest_time)
            self.page_num += 1
            data = {'page': str(self.page_num), 'limit': '20'}
            yield scrapy.FormRequest(url=self.jieban_fenye_url, formdata=data, callback=self.parse_jieban,
                                     meta={'fenye': True, 'entry_id': entry_id})

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
        items = response.xpath('//div[@class="bbs_detail_list"]/div')
        parent_url = response.meta['parent_url']
        for item in items:
            # 发表时间
            post = PostItem()
            post_time = ''.join(item.xpath('.//div[@class="bbs_detail_title clearfix"]/p/text()').extract()).encode('utf-8').split(' ')
            post_time.pop(0)
            post_time = ' '.join(post_time)
            post_time = dateformatting.parse(post_time).strftime('%Y-%m-%d %H:%M:%S')
            # logger.info(post_time)
            site_type = 15
            # logger.info(post_time)
            target = ''.join(item.xpath('.//div[@class="bbs_detail_title clearfix"]/a/text()').extract()).encode('utf-8')
            target = target.replace('\n', '').replace('\t', '').replace('\r', '')
            url = response.url + '#' + target
            author_name = item.xpath('.//h3[@class="titles"]/a/text()').extract()
            if author_name == []:
                continue
            author_name = ''.join(author_name).encode('utf-8')
            text = item.xpath('.//td[@class="editor bbsDetailContainer"]//text()').extract()
            if text == []:
                text = item.xpath('.//ul[@class="xpc"]//text()').extract()
            text = ''.join(text).encode('utf-8')
            img_url = item.xpath('.//td[@class="editor bbsDetailContainer"]//@data-original').extract()
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
            if '#1楼' in url:
                title = response.xpath('//h3[@class="b_tle"]/text()').extract()[-1]
                title = ''.join(title)
                read_num = ''.join(response.xpath('//span[@class="viewtxt"]/text()').extract())
                if read_num == '':
                    read_num = ''.join(response.xpath('//span[@class="poi"]/text()').extract())
                read_num = ''.join(re.findall(r'\d+', read_num))
                try:
                    comment_num = response.meta["comment_num"]
                    comment_num = ''.join(re.findall(r'\d+', comment_num))
                except Exception as e:
                    comment_num = 0
                    logger.info('comment_num Exception: %s' % e)
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
            post['entry_id'] = entry_id
            post['include_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            # 匹配user
            uid = item.xpath('.//h3[@class="titles"]/a/@href').extract()
            uid = ''.join(re.findall(r'/u/(\d+)', uid[0]))
            post['author_id'] = uid
            logger.info('post:%s' % post)
            yield post
            # 匹配user
            yield Request(self.user_url.format(uid), callback=self.parse_user,
                          meta={"entry_id": entry_id, 'uid': uid, 'author_name': author_name})
            # # 匹配detail分页
            detail_urls = response.xpath('//div[@class="ui_page"]/a/@href').extract()
            for detail_url in detail_urls:
                yield Request(response.urljoin(detail_url), callback=self.parse_content,
                              meta={'parent_url': parent_url, 'entry_id': entry_id})

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
