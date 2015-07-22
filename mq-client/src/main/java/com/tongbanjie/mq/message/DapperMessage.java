package com.tongbanjie.mq.message;

/**
 * Created by xiafeng
 * on 2015/7/21.
 */
public class DapperMessage extends Message{

    private static final long serialVersionUID = 5245159546236462792L;

    private byte[] body;

    private byte[] attachment;

    private String msgId;// 消息ID

    private int queueId;// 队列ID <PUT>

    private long queueOffset;// 队列偏移量

    public DapperMessage(){}

    public DapperMessage(byte[] body,byte[] attachment){
        super();
        this.body = body;
        this.attachment = attachment;
    }

    public DapperMessage(String keys, byte[] body,byte[] attachment){
        super(keys,"");
        this.body = body;
        this.attachment = attachment;
    }

    public DapperMessage(String keys,String tags, byte[] body,byte[] attachment){
        super(keys,tags);
        this.body = body;
        this.attachment = attachment;
    }

    public byte[] getAttachment() {
        return attachment;
    }

    public void setAttachment(byte[] attachment) {
        this.attachment = attachment;
    }

    public byte[] getBody() {

        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }
}
