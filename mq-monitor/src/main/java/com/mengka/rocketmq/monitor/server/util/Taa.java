package com.mengka.rocketmq.monitor.server.util;

import com.alibaba.fastjson.JSONObject;

/**
 * User: mengka
 * Date: 15-6-20-обнГ8:23
 */
public class Taa {

    public static void main(String[] args){

        String log = "#mq-monitor-{\"sendMessage\":\"73C002BE00002A9F0000000001D0F9B8\"}#";
        JSONObject jsonObject = MonitorUtil.getMyLog(log);
        if(jsonObject!=null){
            System.out.println(jsonObject.get("sendMessage"));
        }
    }

}
