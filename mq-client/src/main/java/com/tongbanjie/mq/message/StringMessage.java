package com.tongbanjie.mq.message;

/**
 * User: mengka
 * Date: 15-6-20-上午10:04
 */
public class StringMessage extends Message {

    private static final long serialVersionUID = 4184758718796140785L;

    private String body;

    public StringMessage(String body){
        super();
        this.body = body;
    }

    public StringMessage(String keys, String body){
        super(keys,"");
        this.body = body;
    }

    public StringMessage(String keys,String tags, String body){
        super(keys,tags);
        this.body = body;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}