package com.tongbanjie.rocketmq.monitor.constant;

/**
 * User: mengka
 * Date: 15-8-1-����8:14
 */
public enum MonitorKeyGroup {

    LOG_SENDMESSAGE("sendMessage", "������Ϣ��־���");

    private String key;
    private String value;

    private MonitorKeyGroup(String key, String value) {
        this.value = value;
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }

    public static MonitorKeyGroup valueOfKey(String key) {
        for (MonitorKeyGroup tmp : values()) {
            if (tmp.key.equals(key)) {
                return tmp;
            }
        }
        throw new IllegalArgumentException(
                "Invaild value of MonitorKeyGroup");
    }

}
