package com.tongbanjie.mq.message;

import java.io.Serializable;

/**
 * Created by xiafeng
 * on 2015/4/22.
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 8445773977080406428L;

    private String keys;

    private int flag;

    private String tags;

    /**
     * 消息体
     */
    private byte[] body;


    public Message() {
    }

    public Message(String tags, String keys, int flag, byte[] body) {
        this.tags = tags;
        this.flag = flag;
        this.body = body;
        this.keys = keys;
    }

    public Message(byte[] body) {
        this( "", "", 0, body);
    }

    public Message(byte[] body, String tags) {
        this( tags, "", 0, body);
    }

    public Message(String keys, byte[] body) {
        this( "", keys, 0, body);
    }

    public Message(String tags, String keys, byte[] body) {
        this( tags, keys, 0, body);
    }


    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
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
