package com.alibaba.rocketmq.store.schedule;

import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * 延时队列处理task
 * <p/>
 * SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX"
 * <p/>
 * Created by xiafeng
 * on 16-1-2.
 */
class DeliverDelayedMessageTimerTask extends TimerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    // 定时器
    private final Timer timer = new Timer("ScheduleMessageTimerThread", true);

    /**
     * queueId = delayLevel - 1
     */
    private int delayLevel;

    private final long offset;

    /**
     * level
     * /*  offset
     */
    private ConcurrentHashMap<Integer, Long> offsetTable;

    /**
     * level
     * delay timeMillis
     */
    private ConcurrentHashMap<Integer, Long> delayLevelTable;

    /**
     * 存储顶层对象
     */
    private DefaultMessageStore defaultMessageStore;

    private Semaphore semaphore;

    private boolean isController;

    public DeliverDelayedMessageTimerTask(int delayLevel, long offset, ConcurrentHashMap<Integer, Long> offsetTable, ConcurrentHashMap<Integer, Long> delayLevelTable, DefaultMessageStore defaultMessageStore, Semaphore semaphore) {
        this.delayLevel = delayLevel;
        this.offset = offset;
        this.offsetTable = offsetTable;
        this.delayLevelTable = delayLevelTable;
        this.defaultMessageStore = defaultMessageStore;
        this.semaphore = semaphore;
        this.isController = true;
    }

    public DeliverDelayedMessageTimerTask(int delayLevel, long offset, ConcurrentHashMap<Integer, Long> offsetTable, ConcurrentHashMap<Integer, Long> delayLevelTable, DefaultMessageStore defaultMessageStore, Semaphore semaphore, boolean isController) {
        this.delayLevel = delayLevel;
        this.offset = offset;
        this.offsetTable = offsetTable;
        this.delayLevelTable = delayLevelTable;
        this.defaultMessageStore = defaultMessageStore;
        this.semaphore = semaphore;
        this.isController = isController;
    }

    @Override
    public void run() {
        if (isController) {
            run_controller();
        } else {
            run_notController();
        }
    }

    public void run_controller() {
        try {
            Long timeDelay = this.delayLevelTable.get(this.delayLevel);
            if (timeDelay == null) {
                return;
            }
            //acquire
            semaphore.acquire();
            //execute schedule task
            this.executeOnTimeup();
        } catch (Exception e) {
            log.error("DeliverDelayedMessageTimerTask run_controller error!", e);
        } finally {
            semaphore.release();
        }
    }

    public void run_notController() {
        try {
            Long timeDelay = this.delayLevelTable.get(this.delayLevel);
            if (timeDelay == null) {
                return;
            }
            //execute schedule task
            this.executeOnTimeup();
        } catch (Exception e) {
            log.error("DeliverDelayedMessageTimerTask run_notController error!", e);
        }
    }

    /**
     * 纠正下次投递时间，如果时间特别大，则纠正为当前时间
     *
     * @return
     */
    private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {
        // 如果为0，则会立刻投递
        long result = deliverTimestamp;
        // 超过最大值，纠正为当前时间
        long maxTimestamp = now + this.delayLevelTable.get(this.delayLevel);
        if (deliverTimestamp > maxTimestamp) {
            result = now;
        }

        return result;
    }


    public void executeOnTimeup() {
        ConsumeQueue consumeQueue =
                this.defaultMessageStore.findConsumeQueue(ScheduleMessageService.SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
        if (consumeQueue == null) {
            return;
        }
        log.info("-----schedule----- DeliverDelayedMessageTimerTask executeOnTimeup! TOPIC = {} , queueId = {}", consumeQueue.getTopic(), consumeQueue.getQueueId());

        long failScheduleOffset = offset;

        if (consumeQueue != null) {
            SelectMapedBufferResult bufferResult = consumeQueue.getIndexBuffer(this.offset);
            if (bufferResult != null) {
                try {
                    executeOnTimeup_bufferResult(bufferResult);
                } finally {
                    // 必须释放资源
                    bufferResult.release();
                }
            } // end of if (bufferCQ != null)
            else {
                    /*
                     * 索引文件被删除，定时任务中记录的offset已经被删除，会导致从该位置中取不到数据，
                     * 这里直接纠正下一次定时任务的offset为当前定时任务队列的最小值
                     */
                long cqMinOffset = consumeQueue.getMinOffsetInQuque();
                if (offset < cqMinOffset) {
                    failScheduleOffset = cqMinOffset;
                    log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + consumeQueue.getQueueId());
                }
            }
        }

        //出错后在处理一次
        if (isController) {
            timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                    failScheduleOffset, this.offsetTable, this.delayLevelTable, this.defaultMessageStore, null, false), 0);
        }
    }

    public void executeOnTimeup_bufferResult(SelectMapedBufferResult bufferResult) {
        long nextOffset = offset;
        int i = 0;
        for (; i < bufferResult.getSize(); i += ConsumeQueue.CQStoreUnitSize) {
            long tagsCode = bufferResult.getByteBuffer().getLong();
            nextOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);
            long countdown = getCountDown(tagsCode);
            log.info("executeOnTimeup_bufferResult countdown = {} , tagsCode = {}", countdown, tagsCode);

            /**
             * 时间到了，该投递;
             * 时候未到，继续定时;
             */
            if (countdown <= 0) {
                putMessage(bufferResult, nextOffset);
            } else {
                if (isController) {
                    this.timer.schedule(
                            new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset, this.offsetTable, this.delayLevelTable, this.defaultMessageStore, null, false),
                            0);
                    this.updateOffset(this.delayLevel, nextOffset);
                    return;
                }
            }
        }

        nextOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);
        if (isController) {
            this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, nextOffset, this.offsetTable, this.delayLevelTable, this.defaultMessageStore, null, false), 0);
        }
        this.updateOffset(this.delayLevel, nextOffset);
        return;
    }

    /**
     * 时间到了，投递消息
     */
    public void putMessage(SelectMapedBufferResult bufferResult, long nextOffset) {
        long offsetPy = bufferResult.getByteBuffer().getLong();
        int sizePy = bufferResult.getByteBuffer().getInt();

        MessageExt msgExt = this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);

        if (msgExt != null) {
            try {
                /**
                 *  TODO:
                 */
                log.info(String.format("-----schedule----- putMessage msgExt<%s,%s> , msgId = %s , content = %s", offsetPy, sizePy, msgExt.getMsgId(), new String(msgExt.getBody())));
                MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                PutMessageResult putMessageResult = this.defaultMessageStore.putMessage(msgInner);

                if (putMessageResult != null
                        && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    return;
                } else {
                    log.error("ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                            msgExt.getTopic(), msgExt.getMsgId());
                    if (isController) {
                        this.timer.schedule(
                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                        nextOffset, this.offsetTable, this.delayLevelTable, this.defaultMessageStore, null, false), 0);
                        this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    }
                }
            } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me
                                         * msgExt里面的内容不完整
                                         * ，如没有REAL_QID,REAL_TOPIC之类的
                                         * ，导致数据无法正常的投递到正确的消费队列，所以暂时先直接跳过该条消息
                                         */
                log.error("ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                        + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                        + offsetPy + ",sizePy=" + sizePy, e);
            }
        }
    }

    /**
     * 队列里存储的tagsCode实际是一个时间点
     *
     * @return
     */
    public long getCountDown(long tagsCode) {
        long now = System.currentTimeMillis();
        long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
        long countdown = deliverTimestamp - now;
        return countdown;
    }


    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        // 恢复Topic
        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        // 恢复QueueId
        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }
}
