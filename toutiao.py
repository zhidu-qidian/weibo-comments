# coding:utf-8

import re
import time
import logging
import random
import datetime
import requests
import hashlib
from types import StringTypes, UnicodeType
from bs4 import BeautifulSoup
from db import PG
from str_alg import NGram


class CommentsUpload(object):
    """
    评论上传操作类
    """

    def __init__(self):
        self.pg = PG()

    @staticmethod
    def format_time(t=None):
        """
        格式化时间
        :param t: 时间字符串，若空使用当前utc时间
        :return:
        """
        result = None
        if t is None:
            return datetime.datetime.now()
        try:
            result = datetime.datetime.fromtimestamp(t)
        except Exception as e:
            logging.warning(e, exc_info=True)
        if result is None:
            result = datetime.datetime.now()
        return result

    def unique(cls, news_id, user_name, content):
        """
        创建唯一性约束（根据用户名和评论内容的MD5）
        :param news_id: 新闻id
        :param user_name: 用户名
        :param content: 评论内容
        :return:
        """
        m = hashlib.md5()
        m.update(str(news_id))
        if user_name:
            m.update(user_name)
        if content:
            m.update(content)
        return m.hexdigest()

    def upload_pg(self, comment, docid):
        """
        上传PG操作实现
        :param comment:
        :param docid:
        :return:
        """
        flag = True
        doc = dict()
        doc["cid"] = str(comment["comment_id"]) + "tacey"
        doc["docid"] = docid
        doc["uname"] = comment["user_name"]
        doc["avatar"] = comment["user_logo"]
        doc["ctime"] = self.format_time(comment["create_time"])
        doc["commend"] = comment["like_number"]
        doc["content"] = comment["content"]

        conn = self.pg.getconn()
        cursor = conn.cursor()
        sql_insert = """INSERT INTO commentlist_v2
                        (cid, docid, uname, avatar, ctime, commend, content)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s);"""
        params_insert = (doc["cid"], doc["docid"], doc["uname"],
                         doc["avatar"], doc["ctime"], doc["commend"],
                         doc["content"])
        sql_update = "UPDATE newslist_v2 SET comment=comment+1 WHERE docid=%s"
        params_update = (doc["docid"],)
        try:
            cursor.execute(sql_insert, params_insert)
            cursor.execute(sql_update, params_update)
            conn.commit()
        except Exception as e:
            logging.warning(e, exc_info=True)
            conn.rollback()
            flag = False
        else:
            cursor.close()
        finally:
            self.pg.putconn(conn)
        return flag

    def upload_comment(self, docid, comment):
        """
        上传评论（做一层try-except）
        :param docid: 新闻url
        :param comment: 评论（单条）
        :return:
        """
        try:
            flag = self.upload_pg(comment, docid)
            if flag:
                logging.info("Comment Upload Success")
            else:
                logging.warning("Comment Upload Failed")
            return True
        except Exception as e:
            logging.warning(e, exc_info=True)
            return False

    def upload_comments(self, docid, comments):
        """
        :return:
        """
        for comment in comments:
            result = self.upload_comment(docid, comment)

    @staticmethod
    def show_comments(comments):
        """
        打印抓取到的评论结果，DEBUG时查看
        :param comments: 评论列表
        :return:
        """
        for comment in comments:
            print "User Name:       ", comment["user_name"]
            print "User Logo:       ", comment["user_logo"]
            print "Comment Time:    ", comment["create_time"]
            print "Comment Content: ", comment["content"]
            print "Like Number:     ", comment["like_number"]
            print "Comment ID:      ", comment["comment_id"]
            print "=" * 20


class BaseCommentSpider(object):
    @classmethod
    def get_json(cls, url):
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
        try:
            resp = requests.get(url, headers={"user-agent": ua})
        except Exception as e:
            print e
            return {}
        else:
            return resp.json()

    @classmethod
    def to_unicode(cls, text):
        assert isinstance(text, StringTypes)
        if isinstance(text, UnicodeType):
            return text
        else:
            return text.decode("utf-8")

    @classmethod
    def n_gram(cls):
        pass

    @classmethod
    def get_search_list(cls, keyword):
        pass

    @classmethod
    def get_comment(cls, comment_url):
        pass

    @classmethod
    def comment_filter(cls, comment_content):
        pass

    @classmethod
    def get_comments(cls, news_info):
        pass

    @classmethod
    def select_search(cls, news_list):
        pass


class Toutiao(BaseCommentSpider):
    search_url = "https://is.snssdk.com/api/2/wap/search_content/?keyword={keyword}&iid=11229887037&device_id=35089691486"
    comment_url = "http://www.toutiao.com/api/comment/list/?group_id={group_id}&item_id={item_id}&offset={offset}&count={count}"
    comment_reply_url = "http://www.toutiao.com/api/comment/get_reply/?comment_id={comment_id}&dongtai_id={dongtai_id}&offset={offset}&count={count}"
    offset = 0
    count = 20
    num_re = re.compile("(\d+)")
    group_id = re.compile("\d{19}")
    item_id_re = re.compile("item_id=(.*?)&")

    @classmethod
    def get_search_list(cls, keyword):
        search_url = cls.search_url.format(keyword=keyword)
        json_data = cls.get_json(search_url)
        if not json_data:
            return None
        else:
            count = json_data.get("count", 0)
            if not count or count < 1:
                return None
            html = json_data.get("html")
            soup = BeautifulSoup(html, "lxml")
            sections = soup.select(selector="section")
            search_result = list()
            for section in sections:
                group_id = section.get("data-id")
                if not cls.group_id.match(group_id):
                    continue
                href = section.select(selector="a")[0].get("href")
                item_id = cls.item_id_re.findall(str(href))
                if not item_id:
                    continue
                else:
                    item_id = item_id[0]
                title = section.select(selector="h3")[0].get_text()
                comment_num = section.select(selector="span.label-comment")[0].get_text()
                comment_num = cls.num_re.findall(comment_num)[0]
                search_item = dict()
                search_item["ori_title"] = keyword
                search_item["title"] = title
                search_item["group_id"] = group_id
                search_item["item_id"] = item_id
                search_item["comment_num"] = int(comment_num)
                search_result.append(search_item)
            return search_result

    @classmethod
    def comment_filter(cls, comment_content):
        """
        评论过滤：判断评论中是否有图片、@、转发等信息
        :param comment_content: 评论内容
        :return:
        """
        filter_item = [
            "@",
            "<img",
        ]
        for i in filter_item:
            if i in comment_content:
                return False
        return True

    @classmethod
    def get_comment(cls, comment_url):
        json_data = cls.get_json(comment_url)
        if not json_data:
            return []
        has_more = json_data["data"]["has_more"]
        comments = json_data["data"]["comments"]
        result = []
        for comment in comments:
            _comment = dict()
            _comment["content"] = cls.to_unicode(comment["text"])
            if not cls.comment_filter(_comment["content"]):
                continue
            _comment["user_name"] = cls.to_unicode(comment["user"].get("name"))
            _comment["user_logo"] = cls.to_unicode(comment["user"].get("avatar_url"))
            _comment["create_time"] = float(comment["create_time"])
            _comment["like_number"] = comment["digg_count"]
            _comment["comment_id"] = comment["id"]
            result.append(_comment)
        return result, has_more

    @classmethod
    def get_comments(cls, news_info):
        count = 0
        has_more = True
        total_comments = list()
        while has_more:
            group_id = news_info.get("group_id")
            item_id = news_info.get("item_id")
            comment_url = cls.comment_url.format(group_id=group_id,
                                                 item_id=item_id,
                                                 offset=count * cls.count,
                                                 count=cls.count)
            comments, has_more = cls.get_comment(comment_url)
            total_comments.extend(comments)
            count += 1
        return total_comments

    @classmethod
    def select_search(cls, news_list):
        sorted_list = sorted(news_list,
                             key=lambda x: NGram.compare(x["ori_title"],
                                                         x["title"]),
                             reverse=True)
        selected = sorted_list[0]
        return selected


class CommentTask(object):
    def __init__(self):
        self.uploader = CommentsUpload()
        self.pg = PG()
        self.last_ts = datetime.datetime.now() - datetime.timedelta(minutes=10)

    def get_hot_news(self):
        """
        获取待抓取评论的热点新闻
        :return: 新闻列表（url,标题，插入时间）
        """
        news_last = []
        while True:
            current_ts = datetime.datetime.now()
            conn = self.pg.getconn()
            cursor = conn.cursor()
            sql = """select nid,title,docid from newslist_v2
                     where nid in (select nid from newsrecommendhot where
                     ctime >= %s and ctime < %s)"""
            params = (self.last_ts, current_ts)
            try:
                cursor.execute(sql, params)
                for news in cursor.fetchall():
                    item = dict()
                    item["nid"] = news[0]
                    item["title"] = news[1]
                    item["docid"] = news[2]
                    news_last.append(item)
            except Exception as e:
                logging.warning(e)
            else:
                cursor.close()
            finally:
                self.pg.putconn(conn)
            self.last_ts = current_ts
            if news_last:
                break
            else:
                time.sleep(60)
        return news_last

    def get_news(self):
        return [
            {"title": "鹿晗首次演唱《如果》",
             "nid": 0,
             "docid": 'https://kuaibao.qq.com/s/20170409G01WXA00'}
        ]

    def get_comment(self, title):
        search_list = Toutiao.get_search_list(title)
        top_select = Toutiao.select_search(search_list)
        try:
            logging.info("[Toutiao-Title]" + top_select.get("title", "Not Get Toutiao-Comment"))
        except Exception as e:
            logging.error(e)
        comments = Toutiao.get_comments(top_select)
        return comments

    def upload_comment(self, docid, comments):
        # self.uploader.show_comments(comments)
        self.uploader.upload_comments(docid=docid, comments=comments)

    def run(self, sleep_min, sleep_max):
        logging.info("->To get the hot news")
        news_list = self.get_hot_news()
        logging.info("->To get comment")
        for news in news_list:
            title = news.get("title")
            docid = news.get("docid")
            logging.info(title)
            comments = self.get_comment(title)
            self.upload_comment(docid, comments)
            time.sleep(random.randint(sleep_min, sleep_max))

    def test(self, title):
        comments = self.get_comment(title)
        self.uploader.show_comments(comments)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='toutiao_comment.log',
                        filemode='a+')
    logging.info("Start TiuTiao Comment...")
    onece_time_min = 30
    onece_time_max = 120
    task = CommentTask()
    while True:
        try:
            task.run(onece_time_min, onece_time_max)
        except Exception as e:
            logging.error(e)
