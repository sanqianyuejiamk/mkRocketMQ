package com.tongbanjie.rocketmq.monitor.server.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: mengka
 * Date: 15-6-13-����9:52
 */
public class MonitorUtil {

    private static final Logger log = LoggerFactory.getLogger(MonitorUtil.class);

    private static final String MY_LOG = "#mq-monitor-%s#";

    /**
     *  ��ȡ��־����
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
                    .compile("#mq-monitor-([0-9A-Za-z]*)#");
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
     *  ������־����
     *
     * @param key
     * @param content
     * @return
     */
    public static String log(String key,String content){
        if(StringUtils.isBlank(content)){
            return null;
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(key,content);
        String result = JSON.toJSONString(jsonObject);
        return String.format(MY_LOG, result.replace("#",""));
    }

}