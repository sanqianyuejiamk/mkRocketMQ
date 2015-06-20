package com.tongbanjie.rocketmq.monitor.server.observer;

import com.alibaba.fastjson.JSONObject;
import com.tongbanjie.rocketmq.monitor.server.subject.RocketmqSubject;
import com.tongbanjie.rocketmq.monitor.server.util.MonitorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  ���͵�message���ݴ���
 *
 * User: mengka
 * Date: 15-6-13-����9:25
 */
public class MessageObserver implements RocketmqObserver {

    private static final Logger logger = LoggerFactory.getLogger(MessageObserver.class);

    private MessageObserver(){}

    @Override
    public void update(RocketmqSubject subject, Object arg) {
        logger.info("MessageObserver receive subject message = "+arg);
        process(String.valueOf(arg));
    }

    /**
     *  ����message�����־
     *
     * @param content
     */
    public void process(String content){
        JSONObject jsonObject = MonitorUtil.getMyLog(content);
        if(jsonObject.get("message-t")!=null){
            logger.info("--------------, MessageObserver receive key = "+jsonObject.getString("message-t"));
        }
    }

    public static MessageObserver getInitializer(){
        return MessageObserverHolder.messageObserver_Holder;
    }

    private static class MessageObserverHolder{
        private static MessageObserver messageObserver_Holder = new MessageObserver();
    }

}
