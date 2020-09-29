/**
 * $Id: SendMessageRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 *   producer的请求数据封装
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SendMessageRequestHeader implements CommandCustomHeader {

    /**
     *  生产者的groupId
     */
    @CFNotNull
    private String producerGroup;

    /**
     *  消息的topic
     */
    @CFNotNull
    private String topic;

    /**
     *  默认的topic，消息的topic可能还未创建，根据这个已经存在的topic的路由数据，然后发消息
     *  topic="TBW102"
     */
    @CFNotNull
    private String defaultTopic;

    @CFNotNull
    private Integer defaultTopicQueueNums;

    /**
     *  队列id
     */
    @CFNotNull
    private Integer queueId;

    @CFNotNull
    private Integer sysFlag;

    /**
     *  消息的生成时间
     */
    @CFNotNull
    private Long bornTimestamp;

    @CFNotNull
    private Integer flag;

    @CFNullable
    private String properties;

    @CFNullable
    private Integer reconsumeTimes;

    @CFNullable
    private boolean unitMode = false;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getProducerGroup() {
        return producerGroup;
    }


    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getDefaultTopic() {
        return defaultTopic;
    }


    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }


    public Integer getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }


    public void setDefaultTopicQueueNums(Integer defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }


    public Integer getQueueId() {
        return queueId;
    }


    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }


    public Integer getSysFlag() {
        return sysFlag;
    }


    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }


    public Long getBornTimestamp() {
        return bornTimestamp;
    }


    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }


    public Integer getFlag() {
        return flag;
    }


    public void setFlag(Integer flag) {
        this.flag = flag;
    }


    public String getProperties() {
        return properties;
    }


    public void setProperties(String properties) {
        this.properties = properties;
    }


    public Integer getReconsumeTimes() {
        return reconsumeTimes;
    }


    public void setReconsumeTimes(Integer reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }
}
