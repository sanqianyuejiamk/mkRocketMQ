package com.tongbanjie.rocketmq.monitor.constant;

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
}
