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
package com.alibaba.rocketmq.tools.admin;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.RollbackStats;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.*;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.*;
import com.alibaba.rocketmq.tools.admin.api.MessageTrack;


/**
 * 所有运维接口都在这里实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public class DefaultMQAdminExt extends ClientConfig implements MQAdminExt {
    private final DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private String adminExtGroup = "admin_ext_group";
    private String createTopicKey = MixAll.DEFAULT_TOPIC;


    public DefaultMQAdminExt() {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, null);
    }


    public DefaultMQAdminExt(RPCHook rpcHook) {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, rpcHook);
    }


    public DefaultMQAdminExt(final String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException {
        defaultMQAdminExtImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    @Override
    public void createTopic(String key, String newTopic, int readQueueNum, int writeQueueNum, int topicSysFlag) throws MQClientException {
        defaultMQAdminExtImpl.createTopic(key, newTopic, readQueueNum,writeQueueNum, topicSysFlag);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return defaultMQAdminExtImpl.searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.queryMessage(topic, key, maxNum, begin, end);
    }


    @Override
    public void start() throws MQClientException {
        defaultMQAdminExtImpl.start();
    }


    @Override
    public void shutdown() {
        defaultMQAdminExtImpl.shutdown();
    }


    @Override
    public void createAndUpdateTopicConfig(String addr, TopicConfig config) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateTopicConfig(addr, config);
    }


    @Override
    public void createAndUpdateSubscriptionGroupConfig(String addr, SubscriptionGroupConfig config)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateSubscriptionGroupConfig(addr, config);
    }


    @Override
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(String addr, String group) {
        return defaultMQAdminExtImpl.examineSubscriptionGroupConfig(addr, group);
    }


    @Override
    public TopicConfig examineTopicConfig(String addr, String topic) {
        return defaultMQAdminExtImpl.examineTopicConfig(addr, topic);
    }


    @Override
    public TopicStatsTable examineTopicStats(String topic) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineTopicStats(topic);
    }


    @Override
    public ConsumeStats examineConsumeStats(String consumerGroup) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException {
        return examineConsumeStats(consumerGroup, null);
    }


    @Override
    public ConsumeStats examineConsumeStats(String consumerGroup, String topic) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineConsumeStats(consumerGroup, topic);
    }


    @Override
    public ClusterInfo examineBrokerClusterInfo() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        return defaultMQAdminExtImpl.examineBrokerClusterInfo();
    }


    @Override
    public TopicRouteData examineTopicRouteInfo(String topic) throws RemotingException, MQClientException,
            InterruptedException {
        return defaultMQAdminExtImpl.examineTopicRouteInfo(topic);
    }


    @Override
    public void putKVConfig(String namespace, String key, String value) {
        defaultMQAdminExtImpl.putKVConfig(namespace, key, value);
    }


    @Override
    public String getKVConfig(String namespace, String key) throws RemotingException, MQClientException,
            InterruptedException {
        return defaultMQAdminExtImpl.getKVConfig(namespace, key);
    }


    @Override
    public ConsumerConnection examineConsumerConnectionInfo(String consumerGroup)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return defaultMQAdminExtImpl.examineConsumerConnectionInfo(consumerGroup);
    }


    @Override
    public ProducerConnection examineProducerConnectionInfo(String producerGroup, final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineProducerConnectionInfo(producerGroup, topic);
    }


    @Override
    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.wipeWritePermOfBroker(namesrvAddr, brokerName);
    }


    public String getAdminExtGroup() {
        return adminExtGroup;
    }


    public void setAdminExtGroup(String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
    }


    public String getCreateTopicKey() {
        return createTopicKey;
    }


    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }


    @Override
    public List<String> getNameServerAddressList() {
        return this.defaultMQAdminExtImpl.getNameServerAddressList();
    }


    @Override
    public TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchAllTopicList();
    }


    @Override
    public KVTable fetchBrokerRuntimeStats(final String brokerAddr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.fetchBrokerRuntimeStats(brokerAddr);
    }

    @Override
    public void updateTopicLeval(Set<String> addrs, String topic,String group, int level, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        defaultMQAdminExtImpl.updateTopicLeval(addrs,topic,group,level,timeoutMillis);
    }

    @Override
    public void deleteTopicInBroker(Set<String> addrs, String topic) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteTopicInBroker(addrs, topic);
    }


    @Override
    public void deleteTopicInNameServer(Set<String> addrs, String topic) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteTopicInNameServer(addrs, topic);
    }


    @Override
    public void deleteSubscriptionGroup(String addr, String groupName) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteSubscriptionGroup(addr, groupName);
    }


    @Override
    public void createAndUpdateKvConfig(String namespace, String key, String value) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateKvConfig(namespace, key, value);
    }


    @Override
    public void deleteKvConfig(String namespace, String key) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteKvConfig(namespace, key);
    }


    @Override
    public String getProjectGroupByIp(String ip) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getProjectGroupByIp(ip);
    }


    @Override
    public String getIpsByProjectGroup(String projectGroup) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getIpsByProjectGroup(projectGroup);
    }


    @Override
    public void deleteIpsByProjectGroup(String projectGroup) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteIpsByProjectGroup(projectGroup);
    }


    public List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp,
            boolean force) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force);
    }

    @Override
    public List<RollbackStats> resetConsumerOffset_byOffset(String consumerGroup, String topic, boolean force) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetConsumerOffset_byOffset(consumerGroup,topic,force);
    }

    @Override
    public List<RollbackStats> resetConsumerOffset_byOffset(String consumerGroup, String topic, int queueId, boolean force) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetConsumerOffset_byOffset(consumerGroup, topic, queueId, force);
    }


    @Override
    public KVTable getKVListByNamespace(String namespace) throws RemotingException, MQClientException,
            InterruptedException {
        return defaultMQAdminExtImpl.getKVListByNamespace(namespace);
    }


    @Override
    public void updateBrokerConfig(String brokerAddr, Properties properties) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException,
            InterruptedException, MQBrokerException {
        defaultMQAdminExtImpl.updateBrokerConfig(brokerAddr, properties);
    }


    @Override
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp,
            boolean isForce) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestamp(topic, group, timestamp, isForce);
    }


    @Override
    public Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group, String clientAddr)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getConsumeStatus(topic, group, clientAddr);
    }


    @Override
    public void createOrUpdateOrderConf(String key, String value, boolean isCluster)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, isCluster);
    }


    @Override
    public GroupList queryTopicConsumeByWho(String topic) throws InterruptedException, MQBrokerException,
            RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryTopicConsumeByWho(topic);
    }


    @Override
    public Set<QueueTimeSpan> queryConsumeTimeSpan(final String topic, final String group)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryConsumeTimeSpan(topic, group);
    }


    @Override
    public void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQAdminExtImpl.resetOffsetNew(consumerGroup, topic, timestamp);
    }


    @Override
    public boolean cleanExpiredConsumerQueue(String cluster) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueue(cluster);
    }


    @Override
    public boolean cleanExpiredConsumerQueueByAddr(String addr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueueByAddr(addr);
    }


    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId, boolean jstack)
            throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getConsumerRunningInfo(consumerGroup, clientId, jstack);
    }


    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup, String clientId,
            String msgId) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException {
        return defaultMQAdminExtImpl.consumeMessageDirectly(consumerGroup, clientId, msgId);
    }


    @Override
    public List<MessageTrack> messageTrackDetail(MessageExt msg) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.messageTrackDetail(msg);
    }


    @Override
    public void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.cloneGroupOffset(srcGroup, destGroup, topic, isOffline);
    }


    @Override
    public BrokerStatsData ViewBrokerStatsData(String brokerAddr, String statsName, String statsKey)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.ViewBrokerStatsData(brokerAddr, statsName, statsKey);
    }
}
