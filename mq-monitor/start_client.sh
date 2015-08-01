#!/bin/bash

MQ_MONITOR_HOME=/Users/hyy044101331/work_mq_monitor/mq-monitor/mq-monitor/target
m_log=/Users/hyy044101331/logs/rocketmqlogs/broker.log
m_mode=client
m_mport=5679
m_server=127.0.0.1
jar_name=mq-monitor-1.0.0-jar-with-dependencies.jar

cd $MQ_MONITOR_HOME
java -Dlog=$m_log -Dmode=$m_mode -Dmport=$m_mport -Dserver=$m_server -jar $jar_name
