/**
 * $Id: SendMessageResponseHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 *  发送消息的返回结果
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SendMessageResponseHeader implements CommandCustomHeader {

    /**
     *  消息的id
     */
    @CFNotNull
    private String msgId;

    /**
     *  消息的队列id
     */
    @CFNotNull
    private Integer queueId;

    /**
     *  消息的offset
     */
    @CFNotNull
    private Long queueOffset;

    private String transactionId;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }


    public Integer getQueueId() {
        return queueId;
    }


    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }


    public Long getQueueOffset() {
        return queueOffset;
    }


    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
}
