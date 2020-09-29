package com.alibaba.rocketmq.client.component;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.utils.TimeUtil;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.tongbanjie.rediscloud.client.JedisX;
import com.tongbanjie.rediscloud.client.factory.JedisXFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * User: mengka
 * Date: 15-7-25-下午10:06
 */
public class BrokerStoreComponent {

    private static final Logger logger = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private static int timeout = 60 * 1000;

    private static int T_ONE_DAY = 24*60*60;

    public static final String BROKER_MESSAGE_KEY = "broker_innner_msg_%s";

    /**
     * brokerName
     * topicName
     * queueId
     * offset
     */
    public static final String BROKER_MESSAGE_OFFSET_KET = "%s_%s_%s_%s";

    public static final String BROKER_MESSAGE_LAST_OFFSET_KET = "last_offset_%s_%s_%s";

    public static final String BROKER_TOPIC_MSGS_KEY = "new_broker_topic_msgs_%s";

    public static final String BROKER_MESSAGE_CONSUMER_IP = "broker_consumer_ip_%s";//保存消费这条消息的consumer的ip

    private final BrokerConfig brokerConfig = new BrokerConfig();

    protected static JedisX pool;

    private BrokerStoreComponent() {
        try {
            logger.info("brokerStoreComponent {}:{} initialize, redisSwitch = {}",brokerConfig.redisAddr,brokerConfig.redisPort,brokerConfig.redisSwitch);
            if(!brokerConfig.redisSwitch){
                logger.info("brokerStoreComponent initialize redisSwitch is close!");
                return;
            }
            logger.info("brokerStoreComponent initialize ctx , "+brokerConfig.getRocketmqHome() + "/conf/rediscloud.xml");
            pool= new JedisXFactory(brokerConfig.getRocketmqHome() + "/conf/rediscloud.xml", "mengkaPool",true).getClient();
        } catch (Exception e) {
            logger.error("Mkpool init error!", e);
        }
    }

    public Object getObject(String key) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        return pool.getObject(key);
    }

    public void setObject(String key, int expireSecond, Object value) {
        if(!brokerConfig.redisSwitch){
            return;
        }
        pool.setObject(key, expireSecond, value);
    }

    public String get(String key) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        return pool.getString(key);
    }

    public String set(String key, String value) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
            return null;
        }
        return pool.setString(key,0, value);
    }

    public void delete(String key) {
        if(!brokerConfig.redisSwitch){
            return;
        }
        pool.delete(key);
    }

    public void storeMessage(String topic, String msgId,String msgContent,MessageExtBrokerInner msgInner) {
        if(!brokerConfig.redisSwitch){
            logger.info("brokerConfig redisSwitch is close!");
            return;
        }
        //保存消息内容
        setObject(String.format(BrokerStoreComponent.BROKER_MESSAGE_KEY, msgId),T_ONE_DAY, msgInner);
        //topic消息队列
        long nowTime = new Date().getTime();
        String key = String.format(BROKER_TOPIC_MSGS_KEY, topic);
        zaddString(key, nowTime, msgId);
    }

    public MessageExtBrokerInner getMessageBody(String msgId){
        if(StringUtils.isBlank(msgId)){
            return null;
        }
        Object obj = getObject(String.format(BrokerStoreComponent.BROKER_MESSAGE_KEY, msgId));
        if(obj==null){
            return null;
        }
        return (MessageExtBrokerInner)obj;
    }

    public Set<String> getMessages(String topic) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        String key = String.format(BROKER_TOPIC_MSGS_KEY, topic);
        return pool.zrangeByScoreString(key, 0, new Date().getTime());
    }

    public Long zremrangeByScore(String key, double minScore, double maxScore) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        return pool.zremrangeByScore(key, minScore, maxScore);
    }

    public Long zremString(String key, String member) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        return pool.zremString(key, member);
    }

    public Double zscoreString(String key, String member) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        return pool.zscoreString(key, member);
    }

    public List<String> lrangeString(String key) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        return pool.lrangeString(key, 0, -1);
    }

    public Long rpushString(String key, String item) {
        return pool.rpushString(key, item);
    }

    public Long lpushString(String key, String item) {
        return pool.lpushString(key, item);
    }

    public Long lremString(String key, int count, String value) {
        return pool.lremString(key, count, value);
    }

    public void removeMessage(String topic, String value) {
        if(!brokerConfig.redisSwitch){
            return;
        }
        String key = String.format(BROKER_TOPIC_MSGS_KEY, topic);
        if(StringUtils.isBlank(value)){
            return;
        }
        Double score = pool.zscoreString(key, value);
        if(score!=null) {
            pool.zremrangeByScore(key, 0, score);
        }
    }

    /**
     *  获取消息发送时间
     *
     * @param topic
     * @param value
     * @return
     */
    public Double zscoreString_topicMsgs(String topic, String value) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        String msgsKey = String.format(BROKER_TOPIC_MSGS_KEY, topic);
        return pool.zscoreString(msgsKey, value);
    }

    public void removeOneMessage(String topic, String value) {
        if(!brokerConfig.redisSwitch){
            return;
        }
        String key = String.format(BROKER_TOPIC_MSGS_KEY, topic);
        if(StringUtils.isBlank(value)){
            return;
        }
        pool.zremString(key, value);
    }

    public Long ttl(String key) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        return pool.ttl(key);
    }

    public Long incr(String key) {
        if(!brokerConfig.redisSwitch){
            return null;
        }
        if (StringUtils.isBlank(key)) {
            return null;
        }
        return pool.incr(key);
    }

    public void zaddString(String key, double score, String member) {
        if(!brokerConfig.redisSwitch){
            return;
        }
        if (StringUtils.isBlank(key) || StringUtils.isBlank(member)) {
            return;
        }
        pool.zaddString(key, score, member);
    }

    public static BrokerStoreComponent getInitializer() {
        return BrokerStoreComponentHolder.cloudStoreComponent_Holder;
    }

    private static class BrokerStoreComponentHolder {
        private static BrokerStoreComponent cloudStoreComponent_Holder = new BrokerStoreComponent();
    }

    /**
     * 处理一条消息日志
     *
     * @param topic
     * @param time
     * @param msgId
     */
    public void processMessageObserverLog(String topic, Long time, String msgId, String brokerName) {
        if(!brokerConfig.redisSwitch){
            return;
        }
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(msgId) || StringUtils.isBlank(brokerName)) {
            logger.error("processMessageObserverLog params is null!");
            return;
        }
        String key = String.format(Constant.MQ_MONITOR_TOPIC, topic);
        String brokerKey = String.format(Constant.MQ_MONITOR_TOPIC_BROKER, brokerName, topic);
        Date createTime = new Date(time);
        String key2 = String.format(Constant.MQ_MONITOR_TOPIC_TODAY, topic, TimeUtil.toDate(createTime, TimeUtil.format_5));
        logger.info("MessageObserver receive msgId = " + msgId + " , key2 = " + key2);
        zaddString(key, time, msgId);
        zaddString(brokerKey, time, msgId);
        incr(key2);//topic下的每天消息总数
    }
}
