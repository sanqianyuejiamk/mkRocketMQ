package com.alibaba.rocketmq.client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.protocol.ResponseCode;


/**
 * Common Validator
 *
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-28
 */
public class Validators {
    public static final String VALID_PATTERN_STR = "^[%|a-zA-Z0-9_-]+$";
    public static final Pattern PATTERN = Pattern.compile(VALID_PATTERN_STR);
    public static final int CHARACTER_MAX_LENGTH = 255;


    /**
     * @param origin
     * @param pattern
     * @return
     */
    public static boolean regularExpressionMatcher(String origin, Pattern pattern) {
        if (pattern == null) {
            return true;
        }
        Matcher matcher = pattern.matcher(origin);
        return matcher.matches();
    }


    /**
     * @param origin
     * @param patternStr
     * @return
     */
    public static String getGroupWithRegularExpression(String origin, String patternStr) {
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(origin);
        while (matcher.find()) {
            return matcher.group(0);
        }
        return null;
    }


    /**
     * Validate topic
     *
     * @param topic
     * @throws com.alibaba.rocketmq.client.exception.MQClientException
     */
    public static void checkTopic(String topic) throws MQClientException {
        if (UtilAll.isBlank(topic)) {
            throw new MQClientException("the specified topic is blank", null);
        }

        if (!regularExpressionMatcher(topic, PATTERN)) {
            throw new MQClientException(String.format(
                    "the specified topic[%s] contains illegal characters, allowing only %s", topic,
                    VALID_PATTERN_STR), null);
        }

        if (topic.length() > CHARACTER_MAX_LENGTH) {
            throw new MQClientException("the specified topic is longer than topic max length 255.", null);
        }

        //whether the same with system reserved keyword
        if (topic.equals(MixAll.DEFAULT_TOPIC)) {
            throw new MQClientException(
                    String.format("the topic[%s] is conflict with default topic.", topic), null);
        }
    }


    /**
     * Validate group
     *
     * @param group
     * @throws com.alibaba.rocketmq.client.exception.MQClientException
     */
    public static void checkGroup(String group) throws MQClientException {
        if (UtilAll.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }
        if (!regularExpressionMatcher(group, PATTERN)) {
            throw new MQClientException(String.format(
                    "the specified group[%s] contains illegal characters, allowing only %s", group,
                    VALID_PATTERN_STR), null);
        }
        if (group.length() > CHARACTER_MAX_LENGTH) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }
    }


    /**
     * Validate message
     * 1.消息内容不能为空；
     * 2.消息内容大小不能超过128kb；
     *
     * @param msg
     * @param defaultMQProducer
     * @throws com.alibaba.rocketmq.client.exception.MQClientException
     */
    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer)
            throws MQClientException {
        if (null == msg) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
        }
        // topic
        Validators.checkTopic(msg.getTopic());
        // body
        if (null == msg.getBody()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
        }

        if (0 == msg.getBody().length) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
        }

        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL,
                    "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
        }
    }
}
