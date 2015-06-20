package com.tongbanjie.rocketmq.monitor.constant;

import org.apache.commons.lang.StringUtils;

/**
 * Created by mengka
 */
public class Constant {

    /**
     * SOCKET��ȡ���ݵ�ip��ַ
     */
    public static final String DATA_IP = System.getProperty("server");//"127.0.0.1";

    /**
     * SOCKET��ȡ���ݵĶ˿�port
     */
    public static final int DATA_CLIENT_PORT = StringUtils.isNotBlank(System.getProperty("mport"))?Integer.parseInt(System.getProperty("mport")):5679;

    /**
     *  ��־�ļ�
     */
    public static final String LOG_PATH = System.getProperty("log");//"/Users/hyy044101331/logs/rocketmqlogs/broker.log";
}
