package com.tongbanjie.rocketmq.monitor.server.observer;

import com.alibaba.fastjson.JSONObject;
import com.tongbanjie.rocketmq.monitor.server.subject.RocketmqSubject;
import com.tongbanjie.rocketmq.monitor.server.util.MonitorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: mengka
 * Date: 15-6-13-ÏÂÎç9:25
 */
public class MessageObserver implements RocketmqObserver {

    private static final Logger log = LoggerFactory.getLogger(MessageObserver.class);

    @Override
    public void update(RocketmqSubject subject, Object arg) {
        log.info("MessageObserver receive subject message = "+arg);
    }

    public void process(String content){
        JSONObject jsonObject = MonitorUtil.getMyLog(content);

    }

}
