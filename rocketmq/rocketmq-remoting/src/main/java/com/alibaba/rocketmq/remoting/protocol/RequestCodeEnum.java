package com.alibaba.rocketmq.remoting.protocol;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 *  请求类型枚举类
 *
 * Created by xiafeng
 * on 16-1-10.
 */
public enum RequestCodeEnum {

    UPDATE_TOPIC_LEVEL(1215, "UPDATE_TOPIC_LEVEL","设置topic的发送等级"),
    DELETE_TOPIC_IN_BROKER(215, "DELETE_TOPIC_IN_BROKER","从Broker删除Topic配置"),
    DELETE_TOPIC_IN_NAMESRV(216,"DELETE_TOPIC_IN_NAMESRV","从Namesrv删除Topic配置");

    private int value;

    private String name;

    private String describe;

    private RequestCodeEnum(int value, String name) {
        this.name = name;
        this.value = value;
    }

    private RequestCodeEnum(int value, String name,String describe) {
        this.name = name;
        this.value = value;
        this.describe = describe;
    }

    public static RequestCodeEnum valueOf(int value) {
        for (RequestCodeEnum tmp : values()) {
            if (tmp.value == value) {
                return tmp;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getDescribe() {
        return describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    public static String getMessageByValue(Integer value) {
        return valueOf(value).getName();
    }

    public static String getAllMessageByValue(Integer value){
        if(value==null){
            return "";
        }
        RequestCodeEnum requestCodeEnum = valueOf(value);
        if(requestCodeEnum==null){
            return "";
        }
        return new ToStringBuilder(requestCodeEnum).append("value",requestCodeEnum.getValue())
                .append("name", requestCodeEnum.getName())
                .append("describe",requestCodeEnum.getDescribe()).toString();
    }
}
