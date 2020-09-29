package com.alibaba.rocketmq.broker.task;

import java.util.*;
import java.util.concurrent.Semaphore;
import com.alibaba.rocketmq.broker.util.TimeUtil;
import com.alibaba.rocketmq.client.component.BrokerStoreComponent;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.MQConstant;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiafeng
 * on 15-10-6.
 */
public class ReSendTask extends TimerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private static BrokerStoreComponent brokerStoreComponent = BrokerStoreComponent.getInitializer();

    private static String RESEND_TASK_MESSAGE = "resend_task_message_%s_%s";//resend ttl key

    private static String RESEND_TASK_MESSAGE_COUNT = "resend_task_message_count_%s_%s";//message count

    private static String RESEND_TASK_PRE_MESSAGE = "resend_task_pre_message_%s_%s";//pre messageId

    private static final String RESEND_MESSAGE_TAG = "resendTask";//resend消息的key

    private final BrokerConfig brokerConfig = new BrokerConfig();

    private static int T_ONE_DAY = 3*24*60*60;

    private Semaphore semaphore;

    public ReSendTask(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    @Override
    public void run() {
        try{
            log.info("resendTask run..");
            //acquire
            semaphore.acquire();
            //定时任务
            process();
        }catch(Exception e){
            log.error("resendTask run error!",e);
        }finally{
            semaphore.release();
        }
    }

    public void process(){
        List<String> topicList = brokerStoreComponent.lrangeString(MQConstant.MQ_TOPIC_LIST);
        if(topicList==null||topicList.size()<=0){
            return;
        }
        for(String topic:topicList){
            processTopic(topic);
        }
    }

    public void processTopic(String topic){
        if(StringUtils.isBlank(topic)){
            return;
        }

        //TODO: resendTask productGroup
        String producerGroup = "RESEND_TASK_P_GROUP";

        Object nameServerAddr = brokerConfig.getStartNamesrvAddr();
        if(nameServerAddr==null){
            log.error("resendTask process nameServerAddr is null!");
            return;
        }

        //resend messages
        Set<String> list = brokerStoreComponent.getMessages(topic);
        if(list==null||list.size()<=0){
            return;
        }

        //producer for this topic
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(String.valueOf(nameServerAddr));
        try {
            producer.start();
            //resend messages
            for(String msgId:list){
                resendMessage(topic,producer,msgId);
            }
        } catch (Exception e) {
            log.error("resendTask process error!", e);
        } finally {
            producer.shutdown();
        }
    }

    public void resendMessage(String topic,DefaultMQProducer producer,String msgId){
        try {
            //重发频率控制
            MessageExtBrokerInner inner = brokerStoreComponent.getMessageBody(msgId);

            //是否存在preMsgId
            boolean isResendMessageTTL = false;
            String preKey = String.format(RESEND_TASK_PRE_MESSAGE, topic, msgId);
            Object preObj = brokerStoreComponent.getObject(preKey);
            if(preObj==null){
                if(isFirstDeadline(topic,msgId)){
                    isResendMessageTTL = true;
                }
            }else{
                isResendMessageTTL = isResendMessageTTL(topic,String.valueOf(preObj));
            }

            if(!isResendMessageTTL){
                log.info("resendMessage isResendMessageTTL is false!");
                return;
            }
            if(inner==null){
                log.info("resendMessage inner is null! msgId = {}",msgId);
                brokerStoreComponent.removeOneMessage(topic, msgId);
                return;
            }
            //发送消息
//            log.info("resendMessage message topic = {} , id = {}",topic,msgId);
            sendMessage(topic, inner.getTags(), producer, inner.getBody(), RESEND_MESSAGE_TAG,msgId);
        }catch (Exception e){
            log.error("resendMessage error! msgId = "+msgId,e);
        }
    }

    public boolean isFirstDeadline(String topic,String msgId){
        boolean isFirstDeadline = false;
        Double score = brokerStoreComponent.zscoreString_topicMsgs(topic, msgId);
        if(score==null){
            return isFirstDeadline;
        }
        Date sendTime = new Date(Math.round(score));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sendTime);
        calendar.add(calendar.MINUTE, 3);
        Date threeMinuteAfterTime = calendar.getTime();
        if(threeMinuteAfterTime.before(new Date())){
            isFirstDeadline = true;
        }
//        log.info("----------, sendTime = {} , twoMinuteAfterTime = {} , isFirstDeadline = {}", TimeUtil.toDate(sendTime,TimeUtil.format_1),TimeUtil.toDate(twoMinuteAfterTime,TimeUtil.format_1),isFirstDeadline);
        return isFirstDeadline;
    }

    public void sendMessage(String topic, String tag, DefaultMQProducer producer,byte[] content,String key,String oldMsgId) throws Exception {
        Message msg = new Message(topic,
                tag,
                key,
                content);
        //send message
        SendResult result = producer.send(msg);
        //store preMsgId
        storePreMessageId(topic, result.getMsgId(), oldMsgId);
        //delete oldMsgId from list
        brokerStoreComponent.removeOneMessage(topic,oldMsgId);
        log.info("resendTask send message , [ {} =>> {} ] , result = {}", oldMsgId,result.getMsgId(), result.getSendStatus());

        /**
         *  TODO: JUST FOR TEST
         */
        Set<String> list = brokerStoreComponent.getMessages(topic);
        log.info("resendTask updateRedisStat getMessages topic = {} , list = {}", topic, list);
    }

    /**
     *  保存preMsgId
     *
     * @param topic
     * @param resendMsgId
     * @param oldMsgId
     */
    public void storePreMessageId(String topic,String resendMsgId,String oldMsgId){
        if(StringUtils.isBlank(resendMsgId)){
            log.error("resendMsgId is null! {}",oldMsgId);
            return;
        }
        String preKey = String.format(RESEND_TASK_PRE_MESSAGE,topic,resendMsgId);
        Object oldObject = brokerStoreComponent.getObject(String.format(RESEND_TASK_PRE_MESSAGE, topic, oldMsgId));
        if(oldObject==null){
            //preMsg ttl
            String k1 = String.format(RESEND_TASK_MESSAGE,topic,oldMsgId);
            brokerStoreComponent.setObject(k1, T_ONE_DAY, oldMsgId);
            //消息的resend count
            String k2 = String.format(RESEND_TASK_MESSAGE_COUNT,topic,oldMsgId);
            brokerStoreComponent.incr(k2);
            //store preMsgId
            brokerStoreComponent.setObject(preKey,T_ONE_DAY,oldMsgId);
        }else{
            String oldPreMsgId = String.valueOf(oldObject);
            //消息的resend count
            String k2 = String.format(RESEND_TASK_MESSAGE_COUNT,topic,oldPreMsgId);
            brokerStoreComponent.incr(k2);
            //store preMsgId
            brokerStoreComponent.setObject(preKey,T_ONE_DAY,oldPreMsgId);
        }
        //oldMsg resend count
        Long v2 = brokerStoreComponent.incr(String.format(RESEND_TASK_MESSAGE_COUNT, topic, oldMsgId));
        log.info("storePreMessageId oldMsg resend count = {}",v2);
    }

    public boolean isResendMessageTTL(String topic,String msgId){
        boolean result = false;
        try {
            if(StringUtils.isBlank(topic)||StringUtils.isBlank(msgId)){
                return result;
            }
            String count = brokerStoreComponent.get(String.format(RESEND_TASK_MESSAGE_COUNT, topic, msgId));
            if(count==null){
                log.info("isResendMessageTTL first is true");
                return true;
            }
            long time = brokerStoreComponent.ttl(String.format(RESEND_TASK_MESSAGE,topic,msgId));
            long minute = (T_ONE_DAY - time)/60;
            double gwMinute = Math.pow(2, Double.parseDouble(count));
            /**
             *  JUST FOR TEST
             *
             */
            log.info("{}:{} , [minute:gwMinute] = {}:{} , count = {}",topic,msgId,minute,gwMinute,count);
            if(minute > gwMinute){
                result = true;
            }
        }catch (Exception e){
            log.error("isResendMessageTTL error!",e);
        }
        return result;
    }
}
