package com.tongbanjie.rocketmq.monitor.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import redis.clients.jedis.JedisPool;

import java.util.Set;

/**
 * User: mengka
 * Date: 15-7-25-����10:06
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
     *  �Ӵ�С����Ԫ��
     *
     * @param key
     * @param maxScore ���ֵ
     * @param minScore ��Сֵ
     * @param offset ��ʼindex��Ĭ�ϴӵ�0����ʼ��count������
     * @param count  ���صĸ���
     * @return
     */
    public Set<String> zrevrangeByScoreString(String key, double maxScore, double minScore, int offset, int count) {
        return redisClient.zrevrangeByScoreString(key,maxScore,minScore,offset,count);
    }

    /**
     *  ��С���󷵻�
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

    public static CloudStoreComponent getInitializer(){
        return CloudStoreComponentHolder.cloudStoreComponent_Holder;
    }

    private static class CloudStoreComponentHolder{
        private static CloudStoreComponent cloudStoreComponent_Holder = new CloudStoreComponent();
    }
}
