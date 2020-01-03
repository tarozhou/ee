#!/bin/bash

cd /data/zhoudongyu/newdoc_explore/
. /etc/profile
echo T7dDQXso | kinit zhoudongyu

/data/zhoudongyu/pyenv27/bin/python newdoc_explore_consumer.py

hadoop fs -rm hdfs://ns/data/mgwh/data_mining/recommend/data/monitor_newdoc_explore.txt

hadoop fs -put ./data/monitor_newdoc_explore.txt hdfs://ns/data/mgwh/data_mining/recommend/data/
