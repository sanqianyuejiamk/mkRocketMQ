#!/bin/bash

ROCKETMQ_HOME="/Users/hyy044101331/work_mk_rocketmq02/RocketMQ"

cd $ROCKETMQ_HOME/rocketmq-remoting
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-common
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-client
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-store
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-srvutil
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-namesrv
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-broker
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-filtersrv
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-tools
mvn clean install
cd $ROCKETMQ_HOME/rocketmq-example
mvn clean install
