package com.mengka.mq.common;

/**
 *  消费类型
 *
 * Created by xiafeng
 * on 2015/4/22.
 */
public enum ConsumerTypeGroup {

    MQ_PUSH_CONSUMER("PUSH","推送"),
    MQ_PULL_CONSUMER("PULL","拉取");

    private String name;

    private String desc;

    private ConsumerTypeGroup(String name,String desc) {
        this.name = name;
        this.desc = desc;
    }

    public static ConsumerTypeGroup valueOfName(String name) {
        for (ConsumerTypeGroup tmp : values())
            if (tmp.name.equals(name)) {
                return tmp;
            }
        throw new IllegalArgumentException("Invaild value of ConsumerTypeGroup");
    }

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }
}
