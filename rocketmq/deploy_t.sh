#!/bin/bash

PROJECT_HOME="/Users/hyy044101331/work/mavenLib/com/alibaba/rocketmq"
ROCKETMQ_HOME="/Users/hyy044101331/java_tools/alibaba-rocketmq"
ROCKETMQ_VERSION="3.2.8"

cd $ROCKETMQ_HOME/lib
rm -rf rocketmq-*

cp $PROJECT_HOME/rocketmq-broker/$ROCKETMQ_VERSION/rocketmq-broker-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-client/$ROCKETMQ_VERSION/rocketmq-client-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-common/$ROCKETMQ_VERSION/rocketmq-common-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-example/$ROCKETMQ_VERSION/rocketmq-example-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-filtersrv/$ROCKETMQ_VERSION/rocketmq-filtersrv-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-namesrv/$ROCKETMQ_VERSION/rocketmq-namesrv-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-remoting/$ROCKETMQ_VERSION/rocketmq-remoting-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-srvutil/$ROCKETMQ_VERSION/rocketmq-srvutil-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-store/$ROCKETMQ_VERSION/rocketmq-store-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
cp $PROJECT_HOME/rocketmq-tools/$ROCKETMQ_VERSION/rocketmq-tools-$ROCKETMQ_VERSION.jar $ROCKETMQ_HOME/lib/
