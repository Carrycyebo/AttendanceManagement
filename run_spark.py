from api.spark.structured_streaming import run_structured_streaming
from api.spark.sparksql import run_spark_sql
from api.spark.sparkRDD import run_spark_RDD
import multiprocessing
import os

os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

#多进程分别运行run_structured_streaming 和run_spark_sql 和run_spark_RDD

def start_spark():

    p1 = multiprocessing.Process(target=run_structured_streaming)
    p2 = multiprocessing.Process(target=run_spark_sql)
    p3 = multiprocessing.Process(target=run_spark_RDD)
    p1.start()
    p2.start()
    p3.start()

if __name__ == '__main__':
    start_spark()