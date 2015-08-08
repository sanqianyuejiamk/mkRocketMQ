package com.tongbanjie.mq.listener;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * User: mengka
 * Date: 15-8-6-20:46
 */
public interface MessageListenerPull extends MessageListener {

    ConsumeConcurrentlyStatus consumeMessage(final MessageExt msg);
}
