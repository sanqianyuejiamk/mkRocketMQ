package com.tongbanjie.rocketmq.monitor.server.observer;

import com.alibaba.fastjson.JSONObject;
import com.tongbanjie.rocketmq.monitor.constant.LogConstant;
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

    private static final Logger logger = LoggerFactory.getLogger(MessageObserver.class);

    private MessageObserver(){}

    @Override
    public void update(RocketmqSubject subject, Object arg) {
        //logger.info("MessageObserver receive subject message = "+arg);
        process(String.valueOf(arg));
    }

    /**
     *  处理message相关日志
     *
     * @param content
     */
    public void process(String content){
        JSONObject jsonObject = MonitorUtil.getMyLog(content);
        if(jsonObject==null){
            return;
        }
        if(jsonObject.get(LogConstant.K_SEND_MESSAGE)!=null){
            logger.info("--------------, MessageObserver receive key = "+jsonObject.getString(LogConstant.K_SEND_MESSAGE));
        }
    }

    public static MessageObserver getInitializer(){
        return MessageObserverHolder.messageObserver_Holder;
    }

    private static class MessageObserverHolder{
        private static MessageObserver messageObserver_Holder = new MessageObserver();
    }

}
