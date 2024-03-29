package com.mengka.rocketmq.monitor.server.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mengka.rocketmq.monitor.constant.LogConstant;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: mengka
 * Date: 15-6-13-下午9:52
 */
public class MonitorUtil {

    private static final Logger log = LoggerFactory.getLogger(MonitorUtil.class);

    private static final String MY_LOG = "#mq-monitor-%s#";

    /**
     *  获取日志内容
     *
     * @param content
     * @return
     */
    public static JSONObject getMyLog(String content){
        try{
            if(StringUtils.isBlank(content)){
                return null;
            }
            Pattern pattern = Pattern
                    .compile("#mq-monitor-(.*)#");
            Matcher matcher = pattern.matcher(content);
            if (matcher.find()) {
                String data = matcher.group(1);
                return JSON.parseObject(data);
            }
        }catch (Exception e){
            log.error("isMyLog error!",e);
        }
        return null;
    }

    /**
     *  生成日志内容
     *
     * @param key
     * @param content
     * @return
     */
    public static String log(String key,String content,String topic,String brokerName){
        if(StringUtils.isBlank(content)){
            return null;
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(key,content);
        jsonObject.put(LogConstant.K_CREATE_TIME,new Date().getTime());
        jsonObject.put(LogConstant.K_TOPIC,topic);
        jsonObject.put(LogConstant.K_BROKER_NAME,brokerName);
        String result = JSON.toJSONString(jsonObject);
        return String.format(MY_LOG, result.replace("#",""));
    }

    public static String log(String type,String[] keys,String... values){
        if(keys.length<=0||values.length<=0){
            return null;
        }
        JSONObject jsonObject = new JSONObject();
        for(int i=0;i<keys.length;i++){
            jsonObject.put(keys[i], values[i]);
        }
        jsonObject.put(LogConstant.K_CREATE_TIME,new Date().getTime());
        String result = JSON.toJSONString(jsonObject);
        return String.format(MY_LOG, result.replace("#",""));
    }

    public static String[] getKeys(){
        String[] keys = new String[4];
        keys[0] = LogConstant.K_SEND_MESSAGE;
        keys[1] = LogConstant.K_TOPIC;
        keys[2] = LogConstant.K_BROKER_NAME;
        keys[3] = LogConstant.K_BROKER_IP;
        return keys;
    }
}
