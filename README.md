# 考勤信息实时统计系统

#### 介绍
基于Spark、Kafka、MySQL、Flask、WebSocket、bootstarp、highcharts的实时考勤信息统计系统。

##### 模块一
Kafka data real-time production (kafka 数据实时生产)
这个模块主要用来模拟在实时的业务系统中，用户数据是实时生产的,并推送到kafka 相应的主题中。

##### 模块二
Structured Streaming data processing (Structured Streaming 数据处理)
Spark
这个模块主要用来实时处理kafka中主题的数据，并将处理后的数据写入新的topic（实时处理）到MySQL（需要持久化的数据）中。

##### 模块三
Flask web service (Flask web 服务)
这个模块主要用来提供web服务，用户可以通过web服务来查看实时考勤信息。

##### 模块四
WebSocket real-time data transmission (WebSocket 实时数据传输)
这个模块主要用来实时传输考勤信息，用户可以通过WebSocket来实时接收考勤信息。

#### 模块五
Highcharts real-time data visualization (Highcharts 实时数据可视化)
这个模块主要用来实时展示考勤信息，用户可以通过Highcharts来实时展示考勤信息。




#### 软件架构
```
AttendanceManagement
├─ api
│  ├─ db
│  │  └─ util.py
│  ├─ kafka
│  │  ├─ consumer.py
│  │  ├─ producer.py
│  │  ├─ students.json
│  │  ├─ util.py
│  ├─ router
│  │  ├─ app.py.old
│  │  ├─ page.py
│  │  └─ websocket.py
│  ├─ service
│  │  ├─ kafka_service.py
│  │  ├─ mysql_service.py
│  │  └─ websocket_service.py
│  └─ spark
│     ├─ sparkRDD.py
│     ├─ sparksql.py
│     └─ structured_streaming.py
├─ conf
│  └─ getConf.py
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
│  │  ├─ bootstrap-4.6.2-dist
│  │  │  ├─ css
│  │  │  │  └─ bootstrap.min.css
│  │  │  └─ js
│  │  │     └─ bootstrap.min.js
│  │  ├─ css
│  │  │  └─ style.css
│  │  ├─ img
│  │  │  └─ bg.jpg
│  │  └─ js
│  │     ├─ charts.js
│  │     ├─ exporting.js
│  │     ├─ highcharts.js
│  │     ├─ jquery-3.1.1.min.js
│  │     ├─ popper.min.js
│  │     ├─ socket.io.js
│  │     └─ socket.io.js.map
│  └─ templates
│     └─ index.html
└─ __init__.py

```



#### 使用说明

通过requirements.txt.安装依赖包
```
pip install -r requirements.txt
```

##### 模块一
```
python run_producer.py
```

##### 模块二
```
python run_spark.py
```

##### 模块三
```
python run_flask.py
```

##### 模块四
```
python run_websocket.py
```

##### 模块五
```
python run_highcharts.py
```


#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码



