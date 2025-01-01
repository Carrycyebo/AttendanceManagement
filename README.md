# 考勤信息实时统计系统

#### 介绍
{**以下是 Gitee 平台说明，您可以替换此简介**
Gitee 是 OSCHINA 推出的基于 Git 的代码托管平台（同时支持 SVN）。专为开发者提供稳定、高效、安全的云端软件开发协作平台
无论是个人、团队、或是企业，都能够用 Gitee 实现代码托管、项目管理、协作开发。企业项目请看 [https://gitee.com/enterprises](https://gitee.com/enterprises)}

#### 软件架构
软件架构说明


#### 使用说明

1.  xxxx
2.  xxxx
3.  xxxx

#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


#### 特技

1.  使用 Readme\_XXX.md 来支持不同的语言，例如 Readme\_en.md, Readme\_zh.md
2.  Gitee 官方博客 [blog.gitee.com](https://blog.gitee.com)
3.  你可以 [https://gitee.com/explore](https://gitee.com/explore) 这个地址来了解 Gitee 上的优秀开源项目
4.  [GVP](https://gitee.com/gvp) 全称是 Gitee 最有价值开源项目，是综合评定出的优秀开源项目
5.  Gitee 官方提供的使用手册 [https://gitee.com/help](https://gitee.com/help)
6.  Gitee 封面人物是一档用来展示 Gitee 会员风采的栏目 [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)



```
AttendanceManagement
├─ api
│  ├─ db
│  │  ├─ util.py
│  ├─ kafka
│  │  ├─ consumer.py
│  │  ├─ producer.py
│  │  ├─ util.py
│  ├─ rounter
│  │  ├─ app.py.old
│  │  ├─ page.py
│  │  ├─ websocket.py
│  └─ spark
│     ├─ sparkRDD.py
│     ├─ sparksql.py
│     ├─ sparktest.py
│     └─ structured_streaming.py
├─ conf
│  ├─ getConf.py
├─ config
│  └─ default.ini
├─ main.py
├─ README.md
├─ requirements.txt
├─ run_flask.py
├─ run_producer.py
├─ test.py
├─ web
│  ├─ static
│  │  ├─ css
│  │  │  └─ style.css
│  │  ├─ img
│  │  └─ js
│  │     ├─ exporting.js
│  │     ├─ highcharts.js
│  │     ├─ jquery-3.1.1.min.js
│  │     ├─ socket.io.js
│  │     └─ socket.io.js.map
│  └─ templates
│     ├─ index.html
│     ├─ index.html.old
│     └─ index1.html.old
├─ __init__.py
```