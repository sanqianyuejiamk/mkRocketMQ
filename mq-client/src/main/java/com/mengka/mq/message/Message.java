package com.mengka.mq.message;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

/**
 * Created by xiafeng
 * on 2015/4/22.
 */
public abstract class Message implements Serializable {

    private static final long serialVersionUID = 8445773977080406428L;

    private String keys;

    private int flag;

    private String tags;

    public Message() {
        this.tags = "";
        this.flag = 0;
        this.keys = "";
    }

    public Message(String keys,String tags) {
        this.tags = StringUtils.isNotBlank(tags)?tags:"";
        this.flag = 0;
        this.keys = StringUtils.isNotBlank(keys)?keys:"";
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }
}
