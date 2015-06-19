package com.tongbanjie.rocketmq.monitor.server.observer;

import com.alibaba.fastjson.JSONObject;
import com.tongbanjie.rocketmq.monitor.server.subject.RocketmqSubject;
import com.tongbanjie.rocketmq.monitor.server.util.MonitorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  发送的message数据处理
 *
 * User: mengka
 * Date: 15-6-13-下午9:25
 */
public class MessageObserver implements RocketmqObserver {

    private static final Logger log = LoggerFactory.getLogger(MessageObserver.class);

    private MessageObserver(){}

    @Override
    public void update(RocketmqSubject subject, Object arg) {
        log.info("MessageObserver receive subject message = "+arg);
    }

    public void process(String content){
        JSONObject jsonObject = MonitorUtil.getMyLog(content);

    }

    public static MessageObserver getInitializer(){
        return MessageObserverHolder.messageObserver_Holder;
    }

    private static class MessageObserverHolder{
        private static MessageObserver messageObserver_Holder = new MessageObserver();
    }

}
