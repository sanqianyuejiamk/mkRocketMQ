#!/bin/bash

MQ_MONITOR_HOME=/Users/hyy044101331/work_mq_monitor/mq-monitor/target
m_log=/Users/hyy044101331/logs/rocketmqlogs/broker.log
m_mode=client
m_mport=5679
m_server=127.0.0.1

cd $MQ_MONITOR_HOME
java -Dlog=$m_log -Dmode=$m_mode -Dmport=$m_mport -Dserver=$m_server -jar mq-monitor-0.0.1-SNAPSHOT-jar-with-dependencies.jar
