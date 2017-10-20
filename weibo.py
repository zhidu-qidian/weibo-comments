# coding:utf-8
"""
微博评论抓取主类
"""

from types import StringTypes, UnicodeType
import requests
import json
import logging
import re
from urllib import quote_plus
from str_alg import MaxSub

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='weibo_comment.log',
                    filemode='a+')


class WeiboClient(object):
    """
    统一处理对weibo.com的访问
    """
    timeout = 25
    session = requests.Session()

    @staticmethod
    def wrap_time(time_text):
        """对weibo的评论时间作适配"""
        month_map = {
            "Jan": "1",
            "Feb": "2",
            "Mar": "3",
            "Apr": "4",
            "May": "5",
            "June": "6",
            "Jun": "6",
            "July": "7",
            "Jul": "7",
            "Aug": "8",
            "Sept": "9",
            "Sep": "9",
            "Oct": "10",
            "Nov": "11",
            "Dec": "12"
        }
        time_info = time_text.split(" ")
        year = time_info[5]
        mon = month_map[time_info[1]]
        day = time_info[2]
        hms = time_info[3]
        result = "{}-{}-{} {}".format(year, mon, day, hms)
        return result

    def get_content(self, url):
        """html请求"""
        content = None
        try:
            req = self.session.get(url, timeout=self.timeout)
            content = req.content
        except requests.exceptions.ConnectTimeout as e:
            content = "{}"
        except Exception as e:
            logging.warning(e)
        return content

    def get_json(self, url):
        """json请求"""
        content = self.get_content(url)
        json_data = dict()
        try:
            json_data = json.loads(content)
        except Exception as e:
            logging.warning(e)
        return json_data

    def to_unicode(self, text):
        assert isinstance(text, StringTypes)
        if isinstance(text, UnicodeType):
            return text
        else:
            return text.decode("utf-8")


class WeiboSearch(WeiboClient):
    """微博搜索：得到评论数最多的微博得weibo id"""
    search_api = ("http://api.weibo.cn/2/cardlist?c=android&s={s}&gsid={gsid}"
                  "&containerid=230926type%3D2%26q%3D{title}%26t%3D0")
    punctuation_re = re.compile(u"[\s+\.\!\/_,\\\|$%^*()?;:+\"\'+——！，。？；<>《》：`、“”~@#￥%……&*（）]+")

    def text_match_check(self, title, content):
        title = self.to_unicode(title)
        content = self.to_unicode(content)
        title = self.content_filter(title)
        content = self.content_filter(content)
        check_result = MaxSub.max_sub_seque_dis(title, content)
        result = dict()
        result["max_sub_str"] = check_result[0]
        result["max_sub_len"] = check_result[1]
        result["title_dis"] = check_result[2]
        result["content_dis"] = check_result[3]
        return result

    def content_filter(self, text):
        nosense_words = [u"独家新闻",
                         u"独家",
                         u"专题",
                         u"专稿",
                         u"特辑"
                         ]
        text = text.strip(u" ")
        text = self.punctuation_re.sub(u"", text)
        for word in nosense_words:
            if text.startswith(word):
                text = text[len(word):]
        return text

    def get_mid_api(self, title, client_info):
        s = client_info.get("s")
        gsid = client_info.get("gsid")
        quote_title = quote_plus(title.replace("\\", " "))
        url = self.search_api.format(title=quote_title,
                                     s=s,
                                     gsid=gsid)
        result = self.get_json(url)
        mids = {}
        try:
            if result.get("errmsg"):
                if result.get('errno') == '1001030042':
                    return None, None
                else:
                    logging.warning(url)
                    return "error"
        except Exception as e:
            logging.warning(e)
        try:
            cards = [card["mblog"] for card in result["cards"][0]["card_group"]]
        except Exception as e:
            logging.warning(e)
            return None
        logging.info("start computing and checking")
        for item in cards:
            num = item["comments_count"]
            if int(num) < 1: continue
            mid = item["mid"]
            text = item.get("text", "")
            c_result = self.text_match_check(title, text)
            if c_result["max_sub_len"] < 3: continue
            title_dis = sum(c_result["title_dis"]) / (1.0 * c_result["max_sub_len"])
            content_dis = sum(c_result["content_dis"]) / (1.0 * c_result["max_sub_len"])
            if content_dis > 10: continue
            mids[mid] = {
                "text": text,
                "title_dis": title_dis,
                "content_dis": content_dis,
                "max_sub_str": c_result["max_sub_str"],
                "max_sub_len": c_result["max_sub_len"],
                "comment_num": num
            }

        if len(mids) == 0: return None
        mids = sorted(mids.items(), key=lambda x: x[1]["max_sub_len"])
        return mids[-1]


class WeiboComment(WeiboClient):
    """微博评论获取"""
    count = 100
    comment_api = ("http://api.weibo.cn/2/comments/build_comments?"
                   "is_show_bulletin=2&c=android&s={s}&id={id}&gsid={gsid}""&count={count}")

    def get_comments_api(self, c_id, client_info):
        """
        通过Weibo Client API获取评论
        """
        s = client_info.get("s")
        gsid = client_info.get("gsid")
        url = self.comment_api.format(id=c_id,
                                      count=self.count,
                                      s=s,
                                      gsid=gsid)
        result = self.get_json(url)
        if not result:
            logging.info(url)
            return "error"
        _comments = result.get("root_comments")
        if _comments:
            logging.info("Get comments success")
        else:
            return list()
        comments = list()
        for _comment in _comments:
            document = dict()
            document["content"] = _comment["text"]
            if not self.comment_filter_api(document["content"]):
                continue
            document["user_name"] = _comment["user"].get("name")
            document["user_logo"] = _comment["user"].get("profile_image_url")
            document["create_time"] = self.wrap_time(_comment["created_at"])
            document["like_number"] = _comment["user"].get("favourites_count")
            document["comment_id"] = _comment["id"]
            comments.append(document)
        return comments

    def comment_filter_api(self, comment_content):
        """
        评论过滤：判断评论中是否有图片、@、转发等信息
        :param comment_content: 评论内容
        :return:
        """
        filter_item = [
            "Comment with pics",
            "@",
            "<img",
            u"转发微博",
            u"图片评论",
        ]
        for i in filter_item:
            if i in comment_content:
                return False
        return True


if __name__ == "__main__":
    text_test = '#专题|：雄县撕票、、\少女案:曾绑架女子"练手" 其逃走未报案'
    c = WeiboSearch()
    c.text_match_check(text_test, text_test)
