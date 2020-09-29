/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.consumer.store;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.component.BrokerStoreComponent;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * Remote storage implementation
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static Logger log = ClientLogger.getLog();

    private static BrokerStoreComponent brokerStoreComponent = BrokerStoreComponent.getInitializer();

    private final MQClientInstance mQClientFactory;
    private final String groupName;
    private final AtomicLong storeTimesTotal = new AtomicLong(0);
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();


    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }


    @Override
    public void load() {
    }


    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }


    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    try {
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        AtomicLong offset = new AtomicLong(brokerOffset);
                        this.updateOffset(mq, offset.get(), false);
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (MQBrokerException e) {
                        return -1;
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return -2;
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }


    /**
     *  updateConsumeOffset，更新消息队列的offset
     *
     * @param mqs
     */
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        final HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();

        if (mqs != null && !mqs.isEmpty()) {
            for (MessageQueue mq : this.offsetTable.keySet()) {
                persistOne(mq,mqs,unusedMQ);
            }
        }

        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    /**
     *  更新一个队列的offset
     *
     * @param mq
     * @param mqs
     * @param unusedMQ
     */
    public void persistOne(MessageQueue mq,Set<MessageQueue> mqs,final HashSet<MessageQueue> unusedMQ) {
        long times = this.storeTimesTotal.getAndIncrement();
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            if (mqs.contains(mq)) {
                try {
                    this.updateConsumeOffsetToBroker(mq, offset.get());
                    if ((times % 12) == 0) {
                        log.info("Group: {} ClientId: {} updateConsumeOffsetToBroker {} , offset = {}", this.groupName,this.mQClientFactory.getClientId(),mq,offset.get());
                    }
                } catch (Exception e) {
                    log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                }
            } else {
                unusedMQ.add(mq);
            }
        }
    }


    @Override
    public void persist(MessageQueue mq) {
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                this.updateConsumeOffsetToBroker(mq, offset.get());
                log.debug("updateConsumeOffsetToBroker {} {}", mq, offset.get());
            } catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }


    /**
     * Update the Consumer Offset, once the Master is off, updated to Slave,
     * here need to be optimized.
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            // TODO Here may be heavily overhead for Name Server,need tuning
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setCommitOffset(offset);

            this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);

            log.info("updateConsumeOffsetToBrokerRedis topic = {} , queueId = {} , offset = {}",mq.getTopic(),mq.getQueueId(),offset);
            updateConsumeOffsetToBrokerRedis(mq.getBrokerName(), mq.getTopic(),mq.getQueueId(),offset);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    private void updateConsumeOffsetToBrokerRedis(String brokerName,String topic,int queueId,long offset){
        try{
            final String offsetKey = String.format(BrokerStoreComponent.BROKER_MESSAGE_OFFSET_KET,brokerName,topic,queueId,offset);
            final String lastOffsetKey = String.format(BrokerStoreComponent.BROKER_MESSAGE_LAST_OFFSET_KET,brokerName,topic,queueId);
            String lastOffset = brokerStoreComponent.get(lastOffsetKey);
            if(StringUtils.isBlank(lastOffset)){
                lastOffset = String.valueOf(offset-1);
            }

            for(int i=Integer.parseInt(lastOffset);i<=offset;i++){
                updateConsumeOffsetToBrokerRedis_Offset(brokerName,topic,queueId,i);
            }
           }catch (Exception e){
            log.error("updateConsumeOffsetToBrokerRedis error! topic = "+topic,e);
        }
    }

    private void updateConsumeOffsetToBrokerRedis_Offset(String brokerName,String topic,int queueId,long offset){
        final String offsetKey = String.format(BrokerStoreComponent.BROKER_MESSAGE_OFFSET_KET,brokerName,topic,queueId,offset);
        String msgId = brokerStoreComponent.get(String.valueOf(offsetKey));
        if(StringUtils.isBlank(msgId)){
            return;
        }
        log.info("updateConsumeOffsetToBrokerRedis_Offset BROKER_MESSAGE_OFFSET_KET = {} , msgId = {}", offsetKey, msgId);
        brokerStoreComponent.removeMessage(topic, msgId);
        brokerStoreComponent.delete(offsetKey);

        /**
         *  TODO: JUST FOR TEST
         */
        Set<String> list = brokerStoreComponent.getMessages(topic);
        log.info("updateConsumeOffsetToBrokerRedis getMessages topic = " + topic + " , list = " + list);
    }


    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            // TODO Here may be heavily overhead for Name Server,need tuning
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());

            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }


    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. mq={}, offsetTableSize={}", mq,
                    offsetTable.size());
        }
    }


    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        Iterator<MessageQueue> iterator = this.offsetTable.keySet().iterator();
        while (iterator.hasNext()) {
            MessageQueue mq = iterator.next();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, this.offsetTable.get(mq).get());
        }
        return cloneOffsetTable;
    }
}
