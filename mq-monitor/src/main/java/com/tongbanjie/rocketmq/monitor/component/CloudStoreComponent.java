package com.tongbanjie.rocketmq.monitor.component;

import com.tongbanjie.rocketmq.monitor.constant.Constant;
import com.tongbanjie.rocketmq.monitor.server.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import redis.clients.jedis.JedisPool;
import java.util.Date;
import java.util.Set;

/**
 * User: mengka
 * Date: 15-7-25-下午10:06
 */
public class CloudStoreComponent {

    private static final Logger logger = LoggerFactory.getLogger(CloudStoreComponent.class);

    private JedisXClient redisClient;

    private CloudStoreComponent(){
        String serviceConfigXMLs[] = new String[]{"jedis.xml"};
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(serviceConfigXMLs);
        JedisPool jedisPool = (JedisPool)context.getBean("jedisPool");
        redisClient = new JedisXClient(jedisPool);
    }

    public Object getObject(String key) {
        return redisClient.getObject(key);
    }

    public void setObject(String key, int expireSecond, Object value) {
        redisClient.setObject(key, expireSecond, value);
    }

    public Long ttl(String key) {
        return redisClient.ttl(key);
    }

    public Long incr(String key) {
        return redisClient.incr(key);
    }

    public void zaddString(String key, double score, String member){
        redisClient.zaddString(key, score, member);
    }

    /**
     *  从大到小返回元素
     *
     * @param key
     * @param maxScore 最大值
     * @param minScore 最小值
     * @param offset 起始index，默认从第0个开始的count个返回
     * @param count  返回的个数
     * @return
     */
    public Set<String> zrevrangeByScoreString(String key, double maxScore, double minScore, int offset, int count) {
        return redisClient.zrevrangeByScoreString(key,maxScore,minScore,offset,count);
    }

    /**
     *  从小到大返回
     *
     * @param key
     * @param fromValue
     * @param toValue
     * @param offset
     * @param count
     * @return
     */
    public Set<String> zrangeByScoreString(String key,Integer fromValue,Integer toValue, int offset, int count){
        return redisClient.zrangeByScoreString(key,fromValue,toValue,offset,count);
    }

    /**
     *  处理一条消息日志
     *
     * @param topic
     * @param time
     * @param msgId
     */
    public void processMessageObserverLog(String topic, Long time, String msgId){
        String key = String.format(Constant.MQ_MONITOR_TOPIC,topic);
        Date createTime = new Date(time);
        String key2 = String.format(Constant.MQ_MONITOR_TOPIC_TODAY,topic,TimeUtil.toDate(createTime,TimeUtil.format_5));
        zaddString(key, time, msgId);
        incr(key2);//topic下的每天消息总数
    }

    public static CloudStoreComponent getInitializer(){
        return CloudStoreComponentHolder.cloudStoreComponent_Holder;
    }

    private static class CloudStoreComponentHolder{
        private static CloudStoreComponent cloudStoreComponent_Holder = new CloudStoreComponent();
    }
}
