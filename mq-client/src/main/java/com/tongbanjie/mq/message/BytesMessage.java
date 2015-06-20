package com.tongbanjie.mq.message;

/**
 * User: mengka
 * Date: 15-6-20-上午10:20
 */
public class BytesMessage extends Message {

    private static final long serialVersionUID = 5245159546236462792L;

    private byte[] body;

    public BytesMessage(byte[] body){
        super();
        this.body = body;
    }

    public BytesMessage(String keys, byte[] body){
        super(keys,"");
        this.body = body;
    }

    public BytesMessage(String keys,String tags, byte[] body){
        super(keys,tags);
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
