package com.tongbanjie.mq.client;

import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.tongbanjie.mq.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by xiafeng
 * on 2015/4/22.
 */
public class NotifyManagerBean implements NotifyManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultNotifyManager.class);

    private String groupId;

    private String name;

    private String topic;

    private String tag;

    private String ctype;//消费类型

    private String namesrvAddr;

    private MessageListenerConcurrently messageListener;

    private DefaultNotifyManager notifyManager;

    private AtomicBoolean inited = new AtomicBoolean(false);

    @Override
    public SendResult sendMessage(Message message) {
        return this.notifyManager.sendMessage(message);
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessage() throws Exception{
        return this.notifyManager.fetchSubscribeMessage();
    }

    @Override
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) throws Exception{
        return this.notifyManager.pullBlockIfNotFound(mq,subExpression,offset,maxNums);
    }

    public synchronized void init() {
        if (inited.get()) {
            return;
        }
        notifyManager = new DefaultNotifyManager(groupId, name,topic,tag,ctype, messageListener,namesrvAddr);
        inited.set(true);
    }

    public synchronized void initProducter() {
        if (inited.get()) {
            return;
        }
        notifyManager = new DefaultNotifyManager(groupId, name,topic,tag,namesrvAddr);
        inited.set(true);
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getCtype() {
        return ctype;
    }

    public void setCtype(String ctype) {
        this.ctype = ctype;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public MessageListenerConcurrently getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
    }
}
