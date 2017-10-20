# 评论抓取脚本

从微博、今日头条搜索新闻标题，选取与标题最为相近的微博或今日头条文章，获取其下面的评论填充到自己的数据库中


+ 微博搜索用于抓取数据库中所有新闻的评论
+ 今日头条用来抓取数据库中热点新闻的评论


请在`db.py`文件中配置如下条目（注意内网外网IP，服务器部署尽量用内网IP）:

+ MONGO数据库地址，鉴权信息
+ REDIS地址，鉴权信息
+ PG数据库地址，鉴权信息


**运行**
```bash
nohup python comments_main.py
```