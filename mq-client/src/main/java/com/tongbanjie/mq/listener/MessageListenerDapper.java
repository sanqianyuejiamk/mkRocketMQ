package com.tongbanjie.mq.listener;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.tongbanjie.mq.message.DapperMessage;
import com.tongbanjie.mq.util.ObjectUtil;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: xiafeng
 * Date: 15-7-22-9:37
 */
public abstract class MessageListenerDapper implements MessageListenerConcurrently {

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(
            List<MessageExt> list,
            ConsumeConcurrentlyContext Context) {
        List<DapperMessage> dapperMessages = convertDapperMessage(list);
        return consumeDapperMessage(dapperMessages, Context);
    }

    public abstract ConsumeConcurrentlyStatus consumeDapperMessage(List<DapperMessage> list,
                                                             ConsumeConcurrentlyContext Context);

    public List<DapperMessage> convertDapperMessage(List<MessageExt> list){
        List<DapperMessage> dapperMessages = new ArrayList<DapperMessage>();
        for(MessageExt messageExt:list){
            DapperMessage dapperMessage = new DapperMessage();
            Map map = (Map) ObjectUtil.changeByte2Object(messageExt.getBody());
            dapperMessage.setAttachment((byte[])map.get("attachment"));
            dapperMessage.setBody((byte[]) map.get("body"));
            dapperMessage.setKeys(messageExt.getKeys());
            dapperMessage.setTags(messageExt.getTags());
            dapperMessage.setMsgId(messageExt.getMsgId());
            dapperMessage.setQueueId(messageExt.getQueueId());
            dapperMessage.setQueueOffset(messageExt.getQueueOffset());
            dapperMessage.setFlag(messageExt.getFlag());
            dapperMessages.add(dapperMessage);
        }
        return dapperMessages;
    }

}
