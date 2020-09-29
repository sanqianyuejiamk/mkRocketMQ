package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;

/**
 * Created by xiafeng
 * on 15-12-6.
 */
public class UpdateTopicLevelRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String topic;

    @CFNotNull
    private String group;

    @CFNotNull
    private int level;

    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
