package com.tongbanjie.rocketmq.monitor.server.observer;

import com.alibaba.fastjson.JSONObject;
import com.tongbanjie.rocketmq.monitor.component.CloudStoreComponent;
import com.tongbanjie.rocketmq.monitor.constant.LogConstant;
import com.tongbanjie.rocketmq.monitor.server.subject.RocketmqSubject;
import com.tongbanjie.rocketmq.monitor.server.util.MonitorUtil;
import org.apache.commons.lang.StringUtils;
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

    private static CloudStoreComponent cloudStoreComponent = CloudStoreComponent.getInitializer();

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
            if(jsonObject.getLong(LogConstant.K_CREATE_TIME)==null|| StringUtils.isBlank(jsonObject.getString(LogConstant.K_TOPIC))){
                return;
            }
            cloudStoreComponent.processMessageObserverLog(jsonObject.getString(LogConstant.K_TOPIC), jsonObject.getLong(LogConstant.K_CREATE_TIME), jsonObject.getString(LogConstant.K_SEND_MESSAGE));
        }
    }

    public static MessageObserver getInitializer(){
        return MessageObserverHolder.messageObserver_Holder;
    }

    private static class MessageObserverHolder{
        private static MessageObserver messageObserver_Holder = new MessageObserver();
    }

}
