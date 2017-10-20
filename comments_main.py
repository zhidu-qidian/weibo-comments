# coding:utf-8
"""
微博评论抓取主控
"""

import hashlib
import datetime
import logging
import time
from multiprocessing import Manager
from multiprocessing import Pool
from db import PG
from db import DB_M
from weibo import WeiboSearch
from weibo import WeiboComment

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='weibo_comment.log',
                    filemode='a+')


class CommentsUpload(object):
    """
    评论上传操作类
    """

    def __init__(self):
        self.pg = PG()
        self.db_m = DB_M.get_default_database()

    @staticmethod
    def format_time(t=None):
        """
        格式化时间
        :param t: 时间字符串，若空使用当前utc时间
        :return:
        """
        f = "%Y-%m-%d %H:%M:%S"
        result = None
        if t is None:
            return datetime.datetime.now()
        try:
            result = datetime.datetime.strptime(t, f)
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
        :param news_url:
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
        :param news_url: 新闻url
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

    def upload_comments(self, news_url, docid, comments, meta):
        """
        评论上传
        :param news_url: 新闻URL
        :param docid: docid
        :param comments: 评论列表
        :param meta: 评论来源微博信息
        :return:
        """
        count = 0
        for comment in comments:
            result = self.upload_comment(docid, comment)
            if result: count += 1
        result = 0 if count > 0 else 1
        self.stat(news_url, result, meta=meta)

    def stat(self, news_url, result, w_account=None, meta=None):
        """
        记录评论抓取结果
        :param news_url:新闻url
        :param result: 处理结果
                       0：得到评论并成功入库
                       1：得到评论入库失败
                       2: 新闻标题太短
                       3：未搜索到有评论微博
                       4：搜索到有评论微博，未获取到评论
                       5: 搜索新闻被Ban
        :return:
        """
        process_time = datetime.datetime.utcnow()
        self.db_m.weibo_comment_stat.insert({"news_url": news_url,
                                             "result": result,
                                             "process_time": process_time,
                                             "w_account": w_account,
                                             "meta": meta})

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


class CommentsTask(object):
    """
    评论抓力任务处理类
    """
    last_ts = datetime.datetime.now()
    pg = PG()
    db_m = DB_M.get_default_database()

    def get_hot_news(self):
        """
        TO-DO:获取带抓取评论的热门/推荐新闻
        :return:
        """
        pass

    def get_news(self):
        """
        获取待抓取评论的新闻
        :return: 新闻列表（url,标题，插入时间）
        """
        news_last = []
        while True:
            current_ts = datetime.datetime.now()
            conn = self.pg.getconn()
            cursor = conn.cursor()
            sql = """select url,title,ctime,docid from newslist_v2
                     where
                     ctime >= %s and ctime < %s
                     and comment=0
                     and chid not in (28,34,1);"""
            params = (self.last_ts, current_ts)
            try:
                cursor.execute(sql, params)
                for news in cursor.fetchall():
                    news_last.append((news[0], news[1], news[2], news[3]))
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

    def get_account_info(self):
        """
        从MONGO获取微博账号信息，用于评论抓取
        :return: 账号信息列表
        """
        logging.info("To Get comm-account info")
        comm_info = self.db_m.weibo_account.find({"type": "comm", "state": True})
        return list(comm_info)


def get_comments(account_queue, news_queue):
    weibo_search = WeiboSearch()
    weibo_comment = WeiboComment()
    comments_upload = CommentsUpload()
    while True:
        news = news_queue.get()
        cookie_comm = account_queue.get()
        account_queue.put(cookie_comm)
        username = cookie_comm.get("username")
        client_info = cookie_comm.get("client_params")
        url = news[0]
        title = news[1]
        docid = news[3]
        log_title = "%s-->%s" % (username.encode("utf-8"), title)
        logging.info(log_title)
        length_title = len(title.strip().decode("utf-8"))
        if length_title < 5:
            comments_upload.stat(url, 2)
            log_title_short = "[Title、 is too short]%s" % title
            logging.warning(log_title_short)
            continue
        search_result = weibo_search.get_mid_api(title=title, client_info=client_info)
        if search_result == "error":
            log_search_ban = "[Search-Ban]%s" % title
            logging.warning(log_search_ban)
            comments_upload.stat(url, 5, username)
            time.sleep(120)
            continue
        if not search_result:
            comments_upload.stat(url, 3, username)
            log_no_comment = "[No Comment]%s" % title
            logging.info(log_no_comment)
            continue
        c_id = search_result[0]
        meta = search_result[1]
        comments = weibo_comment.get_comments_api(c_id=c_id, client_info=client_info)
        if comments == "error":
            comments_upload.stat(url, 4, username, meta=meta)
            log_comment_error = "[COMMENT ERROR]%s" % title
            logging.warning(log_comment_error)
        else:
            comments_upload.upload_comments(url, docid=docid, comments=comments, meta=meta)


def get_news(account_q, news_q):
    comment_task = CommentsTask()
    account_info = comment_task.get_account_info()
    for account in account_info:
        account_q.put(account)
    while True:
        news_list = comment_task.get_news()
        for news in news_list:
            news_q.put(news)
        time.sleep(60)


if __name__ == "__main__":
    logging.info("Script Start...")
    manager = Manager()
    account_queue = manager.Queue()
    news_queue = manager.Queue()
    task_pool = Pool()
    task_pool.apply_async(get_news, args=(account_queue, news_queue))
    for i in range(4):
        task_pool.apply_async(get_comments, args=(account_queue, news_queue))
    task_pool.close()
    task_pool.join()
