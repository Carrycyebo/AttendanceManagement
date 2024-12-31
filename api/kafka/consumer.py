from kafka import KafkaConsumer

consumer = KafkaConsumer(
#    bootstrap_servers = "域名:port", # kafka集群地址
    auto_offset_reset='earliest',
    bootstrap_servers = "zhao:9092",
    group_id = "test",
    )
consumer.subscribe(["attendance_summary"])
for msg in consumer:
    print((msg.value).decode('utf8'))

