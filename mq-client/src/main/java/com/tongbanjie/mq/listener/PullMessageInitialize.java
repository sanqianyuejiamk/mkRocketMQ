package com.tongbanjie.mq.listener;

import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * User: mengka
 * Date: 15-8-6-21:59
 */
public class PullMessageInitialize {

    private ConcurrentLinkedQueue<MessageExt> queue = new ConcurrentLinkedQueue<MessageExt>();

    public void initPull(MessageExt messageExt) {
        queue.add(messageExt);
    }

    public void initPull(List<MessageExt> msgList) {
        if(CollectionUtils.isEmpty(msgList)){
            return;
        }
        for(MessageExt messageExt:msgList){
            initPull(messageExt);
        }
    }

    public MessageExt get() {
        return queue.poll();
    }

    public ConcurrentLinkedQueue<MessageExt> getQueue() {
        return queue;
    }
}
