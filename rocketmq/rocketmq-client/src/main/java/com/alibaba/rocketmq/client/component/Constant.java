package com.alibaba.rocketmq.client.component;

import org.apache.commons.lang.StringUtils;

/**
 * Created by mengka
 */
public class Constant {

    /**
     * SOCKET获取数据的ip地址
     */
    public static final String DATA_IP = System.getProperty("server");//"127.0.0.1";

    /**
     * SOCKET获取数据的端口port
     */
    public static final int DATA_CLIENT_PORT = StringUtils.isNotBlank(System.getProperty("mport"))?Integer.parseInt(System.getProperty("mport")):5679;

    /**
     *  日志文件
     */
    public static final String LOG_PATH = System.getProperty("log");//"/Users/hyy044101331/logs/rocketmqlogs/broker.log";

    /**
     *  topic下当天的消息数
     *     topic,20150801
     */
    public static final String MQ_MONITOR_TOPIC_TODAY = "mq_monitor_topic_today_%s_%s";

    /**
     *  topic下的消息
     *
     */
    public static final String MQ_MONITOR_TOPIC = "mq_monitor_topic_%s";

    /**
     *  broker&topic下的消息
     *
     */
    public static final String MQ_MONITOR_TOPIC_BROKER = "mq_monitor_topic_%s_%s";
}
