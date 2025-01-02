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
用Highcharts real-time data visualization (Highcharts 实时数据可视化)
来实时展示考勤信息，用户可以通过Highcharts来实时展示考勤信息。
用WebSocket real-time data transmission (WebSocket 持续数据传输)
来实时传输考勤信息，用户可以通过WebSocket来实时接收考勤信息。


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

通过requirements.txt.安装依赖包，自行安装pyspark包
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

#### 特点
-多模块化，每个模块负责不同的功能，方便维护和扩展。

-使用Structured Streaming实时处理kafka中的数据，相比于Spark Streaming， Structured Streaming更加简洁和易用。而且，Structured Streaming支持SQL查询，可以方便地查询和处理数据。

-使用Flask提供web服务，用户可以通过web服务来查看实时考勤信息。

-使用Highcharts实时数据可视化，用户可以通过Highcharts来实时展示考勤信息。

-使用WebSocket实时数据传输，建立持久化连接。


#### 不足
-受服务器资源限制，本次kafka均为单节点部署。
-由于计算资源限制，本次Structured Streaming运行时，如设置2s的batch interval，运行时计算会滞后，实际滞后时间就计算机性能而议