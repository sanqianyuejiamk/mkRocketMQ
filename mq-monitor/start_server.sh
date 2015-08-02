#!/bin/bash

MQ_MONITOR_HOME=/Users/hyy044101331/work_mq_monitor/mq-monitor/mq-monitor/target
m_log=/Users/hyy044101331/logs/rocketmqlogs/broker.log
m_mode=server
m_mport=5679
jar_name=mq-monitor-1.0.1.jar

cd $MQ_MONITOR_HOME
java -Dlog=$m_log -Dmode=$m_mode -Dmport=$m_mport -jar $jar_name
