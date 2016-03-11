package com.mengka.mq.listener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.mengka.mq.client.NotifyManager;
import com.mengka.mq.common.MqStoreComponent;
import com.mengka.mq.common.ReconsumeLaterException;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;

/**
 * User: xiafeng
 * Date: 15-8-6
 */
public class PullMessageManager implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(PullMessageManager.class);

    private static final int MAX_NUMS = 32;//一次拉取的最多消息总数

    private static final long TIME_ONE_HOUR = 60 * 60 * 1000;

    private static final long TIME_RECONSUMER = 60 * 1000;

    private static final long TIME_REPULL = 30 * 1000;

    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

    private static MqStoreComponent mqStoreComponent = MqStoreComponent.getMqStoreComponent();

    public static final ExecutorService EXECUTOR_SERVICE = Executors
            .newCachedThreadPool();

    private NotifyManager consumerNotifyManager;

    private MessageListenerPull messageListener;

    private DefaultMQPullConsumer pullConsumer;

    private long spaceTime;

    @Override
    public void afterPropertiesSet() throws Exception {
        Set<MessageQueue> messageQueues = consumerNotifyManager.fetchSubscribeMessage();
        final Semaphore SEMAPHORE = new Semaphore(messageQueues.size());
        for (MessageQueue messageQueue : messageQueues) {
            EXECUTOR_SERVICE.execute(new PullMessageTask(messageQueue,SEMAPHORE,"thread_mq_"+messageQueue.getQueueId()));
        }
        new Thread(new ReconsumerMessageTask(),"thread_mq_reconsumer").start();

        EXECUTOR_SERVICE.shutdown();
    }

    private class ReconsumerMessageTask implements Runnable{

        @Override
        public void run() {
            while (true) {
                try {
                    logger.debug("reconsumerMessageTask start...");
                    List<MessageExt> messageExts = mqStoreComponent.getMessageExts();
                    consumerMessageList(messageExts);

                    Thread.sleep(TIME_RECONSUMER);
                }catch (Exception e){
                    logger.error("ReconsumerMessageTask error!",e);
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
                    logger.debug("reconsumerMessageTask status is null! id = " + msg.getMsgId());
                } else if (status == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
                    logger.debug("reconsumerMessageTask RECONSUME_LATER message id = " + msg.getMsgId());
                } else if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                    logger.debug("reconsumerMessageTask consumeMessage success! msgId = " + msg.getMsgId() + " , status = " + status);
                    mqStoreComponent.update(msg.getMsgId());
                }
            }
        }

        public ConsumeConcurrentlyStatus consumeMessage(MessageExt msg) {
            try {
                return messageListener.consumeMessage(msg);
            }catch (Exception e){
                logger.error("reconsumerMessageTask consumeMessage error! id = "+msg.getMsgId(), e);
            }
            return null;
        }
    }

    /**
     * 每个队列的消息拉取线程
     */
    private class PullMessageTask implements Runnable {

        private MessageQueue messageQueue;//当前队列

        private String taskName;

        private Semaphore semaphore;

        public PullMessageTask(MessageQueue messageQueue,Semaphore semaphore,String taskName) {
            this.messageQueue = messageQueue;
            this.semaphore = semaphore;
            this.taskName = taskName;
        }

        @Override
        public void run() {
            SINGLE_MQ:
            while (true) {
                logger.debug("pullMessageTask running topic = " + messageQueue.getTopic() + " , thread[" + Thread.currentThread().getId() + "] = " + taskName + " , broker = " + messageQueue.getBrokerName() + " , queueId = " + messageQueue.getQueueId());
                try {
                    semaphore.acquire();

                    long offset = getMessageQueueOffset(messageQueue);
                    PullResult pullResult =
                            consumerNotifyManager.pullBlockIfNotFound(messageQueue, null, offset < 0 ? 0 : offset, MAX_NUMS);

                    if (pullResult.getPullStatus() == PullStatus.FOUND) {
                        logger.debug("pullBlockIfNotFound status = " + pullResult.getPullStatus() + " , offset = " + pullResult.getNextBeginOffset());
                        consumerMessageList(pullResult.getMsgFoundList());
                    } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
                        logger.debug("pullBlockIfNotFound-#no new message,quit while#");
//                        break SINGLE_MQ;
                    } else if (pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
                        logger.debug("pullBlockIfNotFound-#NO_MATCHED_MSG#");
                    } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL) {
                        logger.debug("pullBlockIfNotFound-#OFFSET_ILLEGAL#");
                    } else {
                        logger.debug("pullBlockIfNotFound-#other#");
                    }

                    //update offset
                    putMessageQueueOffset(messageQueue, pullResult.getNextBeginOffset());
                    //sleep spaceTime
                    if(spaceTime>0) {
                        spaceTime = spaceTime > TIME_ONE_HOUR ? TIME_ONE_HOUR : spaceTime;
                        spaceTime = spaceTime < TIME_REPULL ? TIME_REPULL : spaceTime;
                        Thread.sleep(spaceTime);
                    }
                } catch (ReconsumeLaterException e) {
                    logger.debug("Reconsume Later...");
                } catch (RemotingTimeoutException e){
//                    logger.debug("nothing new message...");
                }catch (Exception e) {
                    logger.error("pullMessageTask error! ", e);
                }finally {
                    semaphore.release();
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
                    logger.debug("status is null! id = " + msg.getMsgId());
                    mqStoreComponent.saveMessageExt(msg);
                } else if (status == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
                    logger.debug("RECONSUME_LATER message id = " + msg.getMsgId());
                    mqStoreComponent.saveMessageExt(msg);
                } else if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                    logger.debug("consumeMessage success! msgId = " + msg.getMsgId() + " , status = " + status);
                }
            }
        }

        /**
         * update queue offset
         *
         * @param mq
         * @param offset
         */
        private void putMessageQueueOffset(MessageQueue mq, long offset) throws Exception {
            offseTable.put(mq, offset);
            pullConsumer.updateConsumeOffset(mq, offset);
            logger.debug("update offset queue = " + messageQueue.getQueueId() + " , offset = " + offset);
        }

        public ConsumeConcurrentlyStatus consumeMessage(MessageExt msg) {
            try {
                return messageListener.consumeMessage(msg);
            }catch (Exception e){
                logger.error("consumeMessage error! id = "+msg.getMsgId(), e);
            }
            return null;
        }

        private long getMessageQueueOffset(MessageQueue mq) throws Exception {
            Long offset = offseTable.get(mq);
            logger.debug("#offseTable get queue = " + mq.getQueueId() + " , offset = " + offset + "#");
            if (offset != null) {
            	logger.debug("getMessageQueueOffset = " + offset);
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
