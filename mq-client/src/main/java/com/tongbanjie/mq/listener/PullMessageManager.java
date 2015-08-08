package com.tongbanjie.mq.listener;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.tongbanjie.mq.client.NotifyManager;
import com.tongbanjie.mq.common.MqStoreComponent;
import com.tongbanjie.mq.common.ReconsumeLaterException;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import java.util.*;

/**
 * User: mengka
 * Date: 15-8-6-20:00
 */
public class PullMessageManager implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(PullMessageManager.class);

    private static final int MAX_NUMS = 32;//一次拉取的最多消息总数

    private static final long TIME_ONE_HOUR = 60*60*1000;

    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

    private static MqStoreComponent mqStoreComponent = MqStoreComponent.getMqStoreComponent();

    private NotifyManager consumerNotifyManager;

    private MessageListenerPull messageListener;

    private DefaultMQPullConsumer pullConsumer;

    private long spaceTime;

    @Override
    public void afterPropertiesSet() throws Exception {
        Set<MessageQueue> messageQueues = consumerNotifyManager.fetchSubscribeMessage();
        for (MessageQueue messageQueue : messageQueues) {
            new Thread(new PullMessageTask(messageQueue)).start();
        }
    }

    /**
     *  每个队列的消息拉取线程
     *
     */
    private class PullMessageTask implements Runnable {

        private MessageQueue messageQueue;//当前队列

        public PullMessageTask(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {
            logger.info("pullMessageTask running topic = " + messageQueue.getTopic() + " , broker = " + messageQueue.getBrokerName() + " , queueId = " + messageQueue.getQueueId());
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult =
                            consumerNotifyManager.pullBlockIfNotFound(messageQueue, null, getMessageQueueOffset(messageQueue), MAX_NUMS);

                    if (pullResult.getPullStatus() == PullStatus.FOUND) {
                        logger.info("pullBlockIfNotFound status = " + pullResult.getPullStatus() + " , offset = " + pullResult.getNextBeginOffset());
                        consumerMessageList(pullResult.getMsgFoundList());
                    } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
                        logger.info("pullBlockIfNotFound-#no new message,quit while#");
                        break SINGLE_MQ;
                    } else if (pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
                        logger.info("pullBlockIfNotFound-#NO_MATCHED_MSG#");
                    } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL) {
                        logger.info("pullBlockIfNotFound-#OFFSET_ILLEGAL#");
                    } else {
                        logger.info("pullBlockIfNotFound-#other#");
                    }

                    //update offset
                    putMessageQueueOffset(messageQueue, pullResult.getNextBeginOffset());
                    //sleep spaceTime
                    spaceTime = spaceTime<0?0:spaceTime;
                    spaceTime = spaceTime>TIME_ONE_HOUR?TIME_ONE_HOUR:spaceTime;
                    Thread.sleep(spaceTime);
                } catch (ReconsumeLaterException e) {
                    logger.info("Reconsume Later...");
                } catch (Exception e) {
                    logger.error("pullMessageTask error! ", e);
                }
            }
        }

        public void consumerMessageList(List<MessageExt> list) throws Exception {
            if (CollectionUtils.isEmpty(list)) {
                return;
            }
            for (MessageExt msg : list) {
                ConsumeConcurrentlyStatus status = consumeMessage(msg);
                if (status == null) {
                    logger.info("status is null! id = "+msg.getMsgId());
                    mqStoreComponent.saveMessageExt(msg);
                } else if (status == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
                    logger.info("RECONSUME_LATER message id = "+msg.getMsgId());
                    mqStoreComponent.saveMessageExt(msg);
                } else if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                    logger.info("consumeMessage success! msgId = " + msg.getMsgId() + " , status = " + status);
                }
            }
        }

        /**
         * update queue offset
         *
         * @param mq
         * @param offset
         */
        private void putMessageQueueOffset(MessageQueue mq, long offset) throws Exception{
            offseTable.put(mq, offset);
            pullConsumer.updateConsumeOffset(mq, offset);
            logger.info("update offset queue = "+messageQueue.getQueueId()+" , offset = "+offset);
        }

        public ConsumeConcurrentlyStatus consumeMessage(MessageExt msg) {
            return messageListener.consumeMessage(msg);
        }

        private long getMessageQueueOffset(MessageQueue mq) throws Exception {
            Long offset = offseTable.get(mq);
            logger.info("#offseTable get queue = " + mq.getQueueId() + " , offset = " + offset + "#");
            if (offset != null) {
                System.out.println("getMessageQueueOffset = " + offset);
                return offset;
            }
            return pullConsumer.fetchConsumeOffset(mq, false);
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public void setMessageQueue(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

    }

    public NotifyManager getConsumerNotifyManager() {
        return consumerNotifyManager;
    }

    public void setConsumerNotifyManager(NotifyManager consumerNotifyManager) {
        this.consumerNotifyManager = consumerNotifyManager;
        pullConsumer = consumerNotifyManager.getPullConsumer();
    }

    public MessageListenerPull getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListenerPull messageListener) {
        this.messageListener = messageListener;
    }

    public long getSpaceTime() {
        return spaceTime;
    }

    public void setSpaceTime(long spaceTime) {
        this.spaceTime = spaceTime;
    }
}
