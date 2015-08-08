package com.tongbanjie.mq.client;


import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.tongbanjie.mq.message.Message;
import java.util.Set;

public interface NotifyManager {

    /**
     *  发送消息
     */
    public SendResult sendMessage(Message message);

    /**
     *  拉取消息
     *
     * @return
     */
    public Set<MessageQueue> fetchSubscribeMessage()throws Exception;

    /**
     *  pull message
     *
     * @param mq
     * @param subExpression
     * @param offset
     * @param maxNums 一次返回的消息maxNums
     * @return
     * @throws Exception
     */
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)throws Exception;

    public abstract DefaultMQPullConsumer getPullConsumer();

    public abstract DefaultMQPushConsumer getPushConsumer();

    public abstract DefaultMQProducer getProducer();
}
