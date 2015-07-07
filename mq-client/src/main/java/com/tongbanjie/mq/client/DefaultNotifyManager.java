package com.tongbanjie.mq.client;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.tongbanjie.mq.common.ConsumerTypeGroup;
import com.tongbanjie.mq.message.BytesMessage;
import com.tongbanjie.mq.message.Message;
import com.tongbanjie.mq.message.StringMessage;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;

/**
 * Created by xiafeng
 * on 2015/4/22.
 */
public class DefaultNotifyManager implements NotifyManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultNotifyManager.class);

    private String groupId;

    private String name;

    private String topic;

    private String tag;

    private String namesrvAddr;

    private ConsumerTypeGroup ctype;//消费类型

    private MessageListenerConcurrently messageListener;

    private DefaultMQPullConsumer pullConsumer;

    private DefaultMQPushConsumer pushConsumer;

    private DefaultMQProducer producer;

    public DefaultNotifyManager(String groupId, String name, String topic, String tag, String ctype, MessageListenerConcurrently messageListener, String namesrvAddr) {
        this.groupId = groupId;
        this.name = name;
        this.topic = topic;
        this.tag = tag;
        this.namesrvAddr = namesrvAddr;
        this.messageListener = messageListener;
        this.ctype = ConsumerTypeGroup.valueOfName(ctype);
        this.init();
    }

    public DefaultNotifyManager(String groupId, String name, String topic, String tag, String namesrvAddr) {
        this.groupId = groupId;
        this.name = name;
        this.topic = topic;
        this.tag = tag;
        this.namesrvAddr = namesrvAddr;
        this.init_producter();
    }

    private void init() {
        if (ConsumerTypeGroup.MQ_PUSH_CONSUMER == this.ctype) {
            init_pushConsumer();
        } else if (ConsumerTypeGroup.MQ_PULL_CONSUMER == this.ctype) {
            init_pullConsumer();
        }
        log.info("notify client start..");
    }

    private void init_producter(){
        try {
            producer = new DefaultMQProducer(this.groupId);
            producer.setNamesrvAddr(this.namesrvAddr);
            producer.start();
        }catch (Exception e){
            log.error("DefaultNotifyManager init_producter error! groupId = " + groupId, e);
        }
    }


    private void init_pullConsumer() {
        try {
            pullConsumer = new DefaultMQPullConsumer(this.groupId);
            pullConsumer.setNamesrvAddr(this.namesrvAddr);
            pullConsumer.start();
        } catch (Exception e) {
            log.error("DefaultNotifyManager init_pullConsumer error! groupId = " + groupId + " , topic = " + topic, e);
        }
    }

    /**
     *  默认：一个新的订阅组第一次启动从队列的最前位置开始消费
     *
     */
    private void init_pushConsumer() {
        try {
            pushConsumer =
                    new DefaultMQPushConsumer(this.groupId);
            pushConsumer.setNamesrvAddr(this.namesrvAddr);
            pushConsumer.subscribe(topic,
                    tag);
            pushConsumer.setConsumeFromWhere(
                    ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            pushConsumer.registerMessageListener(messageListener);
            pushConsumer.start();
        } catch (Exception e) {
            log.error("DefaultNotifyManager init_pushConsumerr error! groupId = " + groupId + " , topic = " + topic + " , tag = " + tag, e);
        }
    }

    @Override
    public SendResult sendMessage(Message message) {
        SendResult result = null;
        try {
            byte[] body = null;
            if (message instanceof BytesMessage) {
                body = ((BytesMessage) message).getBody();
            }else if(message instanceof StringMessage){
                body = ((StringMessage) message).getBody().getBytes();
            }else {
                throw new IllegalArgumentException("message:" + message.getClass() + " not support");
            }
            com.alibaba.rocketmq.common.message.Message msg = new com.alibaba.rocketmq.common.message.Message(topic,
                    message.getTags(),
                    message.getKeys(),
                    body);
            result = producer.send(msg);
        }catch (Exception e){
            log.error("DefaultNotifyManager sendMessage error! message = "+ ToStringBuilder.reflectionToString(message),e);
        }
        return result;
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessage() throws MQClientException {
        return pullConsumer.fetchSubscribeMessageQueues(this.topic);
    }

    @Override
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pullConsumer.pullBlockIfNotFound(mq,subExpression,offset,maxNums);
    }

    public DefaultMQPullConsumer getPullConsumer()
    {
        return this.pullConsumer;
    }

    public DefaultMQPushConsumer getPushConsumer()
    {
        return this.pushConsumer;
    }

    public DefaultMQProducer getProducer()
    {
        return this.producer;
    }
}
