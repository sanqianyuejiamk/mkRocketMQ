package com.alibaba.rocketmq.client.component;

import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.SafeEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class JedisXClient {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private int limitMillSec2log = 100;
    private JedisPool jedisPool;

    public JedisXClient(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private JedisPool getJedisPool() {
        return this.jedisPool;
    }

    /**
     * while the Response return without limitMillSec2log, will record an log,it contain the Key and the Server Node Address
     *
     * @param limitMillSec2log
     */
    public void setLimitMillSec2log(int limitMillSec2log) {
        this.limitMillSec2log = limitMillSec2log;
    }

    public Object deserialize(byte[] aaByte){
        try {
            if(aaByte==null||aaByte.length<=0){
                return null;
            }
            ByteArrayInputStream byteinInputStream = new ByteArrayInputStream(
                    aaByte);
            ObjectInputStream objectInputStream = new ObjectInputStream(
                    byteinInputStream);

            // ��ȡ����
            Object object = objectInputStream.readObject();

            objectInputStream.close();
            byteinInputStream.close();


            return object;
        }catch (Exception e){
            log.error("deserialize error! e = "+e.getMessage());
        }
        return null;
    }

    public byte[] serialize(Object object) {
        try {
            if(object==null){
                return null;
            }
            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                    byteOutputStream);

            // д��
            objectOutputStream.writeObject(object);

            objectOutputStream.close();
            byteOutputStream.close();

            return byteOutputStream.toByteArray();
        }catch (Exception e){
            log.error("serialize error! e = "+e.getMessage());
        }
        return null;
    }


    private byte[][] getBArrArr(List<String> thisKeys) {
        byte[][] bkeys = new byte[thisKeys.size()][];
        for (int i = thisKeys.size() - 1; i >= 0; i--) {
            bkeys[i] = SafeEncoder.encode(thisKeys.get(i));
        }
        return bkeys;
    }

    private byte[][] getKeyValueBArrArr(List<String> thisKeys, Map<String, byte[]> keyValueMap) {
        byte[][] bKeyValues = new byte[thisKeys.size() * 2][];
        for (int i = 0; i < thisKeys.size(); i++) {
            String key = thisKeys.get(i);
            bKeyValues[i + i] = SafeEncoder.encode(key);
            bKeyValues[i + i + 1] = keyValueMap.get(key);
        }
        return bKeyValues;
    }

    private void valueTypeAssert(Object value) {
        if (value == null) {
//            throw new Exception("nut support the Object-type of Null");
        }
    }

    /********************************above:attr********************************************/

    /*******************************below:expire  ******************************/
    /**
     * ��key���ù���ʱ��
     *
     * @param key
     * @param expireSeconds
     * @return Integer reply, specifically: 1: the timeout was set. 0: the
     *         timeout was not set since the key already has an associated
     *         timeout (this may happen only in Redis versions < 2.1.3, Redis >=
     *         2.1.3 will happily update the timeout), or the key does not
     *         exist.
     */
    public Long expire(String key, int expireSeconds) {
        long begin = System.currentTimeMillis();
        Long ret = expire(getJedisPool(), SafeEncoder.encode(key), expireSeconds);
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        return ret;
    }


    public Long ttl(String key){
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.ttl(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    private Long expire(JedisPool jedisPool, byte[] key, int expireSeconds) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.expire(key, expireSeconds);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }


    /******************************above:expire *************************/

    /*****************************below:set********************************************/
    /**
     * ����һ���ַ���getString(key)��multiGetString(List\<keys\>)���ʹ��,����ɹ����޷�ͨ��getObject(key)ȡ������
     * ��������Ҫ�Դ�String����append��incr��decr������������setObject�滻
     *
     * @param key
     * @param expireSecond ����ʱ�䣬��λΪ�루0�͸����ʾ�����ù��ڣ�
     * @param value
     * @return "OK" or "failed"
     */
    public String setString(final String key, int expireSecond, String value) {
        return setByteArr(key, expireSecond, SafeEncoder.encode(value));
    }

    /**
     * ����һ��������getObject(key)��multiGetObject(List\<keys\>)���ʹ��
     *
     * @param key
     * @param expireSecond ����ʱ�䣬��λΪ�루0�͸����ʾ�����ù��ڣ�
     * @param value        �����л�����ʵ��Serializable�ӿڣ�
     * @return "OK" or "failed"
     */
    public String setObject(final String key, int expireSecond, Object value) {
        return setByteArr(key, expireSecond, serialize(value));
    }

    /**
     * ����һ����ݿ顣��getByteArr(key)��multiGetByteArr(List\<keys\>)���ʹ��
     *
     * @param key
     * @param expireSecond ����ʱ�䣬��λΪ�루0�͸����ʾ�����ù��ڣ�
     * @param value
     * @return "OK" or "failed"
     */
    public String setByteArr(final String key, int expireSecond, byte[] value) {
        long begin = System.currentTimeMillis();
        valueTypeAssert(value);
        String ret = set(getJedisPool(), SafeEncoder.encode(key), expireSecond, value);
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        return ret;
    }

    private String set(JedisPool jedisPool, final byte[] key, int expireSecond, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    String ret = null;
                    if (expireSecond > 0) ret = jedis.setex(key, expireSecond, value);
                    else ret = jedis.set(key, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return "failed";
    }
    /********************************above:set********************************************/

    /**
     * *****************************below:multiget****************************************
     */
    /**
     * һ��ȡ����������String����
     *
     * @param keys
     * @return
     */
    public Map<String, String> multiGetString(List<String> keys) {
        Map<String, String> ret = new HashMap<String, String>();
        Map<String, byte[]> temp = multiGetByteArr(keys);
        for (String key : temp.keySet()) {
            byte[] value = temp.get(key);
            if (value != null) {
                ret.put(key, SafeEncoder.encode(value));
            } else {
                ret.put(key, null);
            }
        }
        return ret;
    }

    /**
     * һ��ȡ����������Object
     *
     * @param keys
     * @return
     */
    public Map<String, Object> multiGetObject(List<String> keys) {
        Map<String, Object> ret = new HashMap<String, Object>();
        Map<String, byte[]> temp = multiGetByteArr(keys);
        for (String key : temp.keySet()) {
            byte[] value = temp.get(key);
            if (value != null) {
                ret.put(key, deserialize(value));
            } else {
                ret.put(key, null);
            }
        }
        return ret;
    }

    /**
     * һ��ȡ������������ݿ�
     *
     * @param keys
     * @return
     */
    public Map<String, byte[]> multiGetByteArr(List<String> keys) {
        if (keys==null||keys.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        Map<String, byte[]> ret = multiGet(getJedisPool(), getBArrArr(keys));
        if (ret == null) {
            return Collections.EMPTY_MAP;
        }
        return ret;
    }

    private Map<String, byte[]> multiGet(JedisPool jedisPool, byte[]... keys) {
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    List<byte[]> ret = jedis.mget(keys);
                    for (int i = ret.size() - 1; i >= 0; i--) {
                        map.put(SafeEncoder.encode(keys[i]), ret.get(i));
                    }
                    return map;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return map;
    }
    /********************************above:multiget*****************************************/

    /**
     * *****************************below:setIfNotExist***********************************
     */
    /**
     * ������setString����ͬ����ֻ���ڴ�key������ʱ�Ż���ɹ�
     *
     * @param key
     * @param expireSecond
     * @param value
     * @return 1��ʾ����ɹ���0��ʾkey�Ѵ��ڻ��治�ɹ���-1��ʾ�������쳣
     */
    public long setStringIfNotExist(final String key, int expireSecond, String value) {
        return setByteArrIfNotExist(key, expireSecond, SafeEncoder.encode(value));
    }

    /**
     * ������setObject����ͬ����ֻ���ڴ�key������ʱ�Ż���ɹ�
     *
     * @param key
     * @param expireSecond
     * @param value
     * @return 1��ʾ����ɹ���0��ʾkey�Ѵ��ڻ��治�ɹ���-1��ʾ�������쳣
     */
    public long setObjectIfNotExist(final String key, int expireSecond, Object value) {
        return setByteArrIfNotExist(key, expireSecond, serialize(value));
    }

    /**
     * ������setByteArr����ͬ����ֻ���ڴ�key������ʱ�Ż���ɹ�
     *
     * @param key
     * @param expireSecond
     * @param value
     * @return 1��ʾ����ɹ���0��ʾkey�Ѵ��ڻ��治�ɹ���-1��ʾ�������쳣
     */
    public long setByteArrIfNotExist(final String key, int expireSecond, byte[] value) {
        long begin = System.currentTimeMillis();
        valueTypeAssert(value);
        long ret = setIfNotExist(getJedisPool(), SafeEncoder.encode(key), expireSecond, value);
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        return ret;
    }

    private long setIfNotExist(JedisPool jedisPool, final byte[] key, int expireSecond, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.setnx(key, value);
                    if (expireSecond > 0) jedis.expire(key, expireSecond);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return -1L;
    }
    /********************************above:setIfNotExist*********************************/


    /*****************************below:get*******************************************/
    /**
     * ȡ��������ַ���setString()���ʹ�á�.
     * ��Ҫע���ʱͨ��setObject(key,str)֮��Ľӿڻ�����ַ�����޷�ͨ��˽ӿ�ȡ����ȷ���
     *
     * @param key
     * @return
     */
    public String getString(String key) {
        long begin = System.currentTimeMillis();

        byte[] ret = get(getJedisPool(), SafeEncoder.encode(key));
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        if (ret != null) return SafeEncoder.encode(ret);
        return null;
    }

    /**
     * ȡ��������ַ���setObject()���ʹ�á�.
     * ��Ҫע���ʱͨ��setString(key,str)֮��Ľӿڻ�����ַ�����޷�ͨ��˽ӿ�ȡ����ȷ���
     *
     * @param key
     * @return
     */
    public Object getObject(String key) {
        long begin = System.currentTimeMillis();

        byte[] ret = get(getJedisPool(), SafeEncoder.encode(key));
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        if (ret != null) return deserialize(ret);
        return null;
    }

    /**
     * ȡ��������ַ���setByteArr()���ʹ�á�.
     *
     * @param key
     * @return
     */
    public byte[] getByteArr(String key) {
        long begin = System.currentTimeMillis();
        byte[] ret = get(getJedisPool(), SafeEncoder.encode(key));
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        return ret;
    }

    public String get(String key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    String ret = jedis.get(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    public String set(String key, String value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    String ret = jedis.set(key, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    private byte[] get(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    byte[] ret = jedis.get(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /********************************above:get********************************************/


    /*****************************below:exists*****************************************/
    /**
     * ��ѯĳkey�Ƿ���ڡ�������ͨ��setString,����setObject��setByteArr���κνӿڻ�������ݣ�ֻҪ���������Ч���ڣ��н�����true
     *
     * @param key
     * @return
     */
    public Boolean exists(String key) {
        long begin = System.currentTimeMillis();

        Boolean ret = exists(getJedisPool(), SafeEncoder.encode(key));
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        return ret;

    }

    private Boolean exists(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Boolean ret = jedis.exists(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return false;
    }
    /********************************above:exists********************************************/

    /**
     * *****************************below:delete,multidelte**********************************
     */
    /**
     * ɾ��ĳ������k-v��
     *
     * @param key
     * @return 0��ʾkey�����ڻ�δɾ��ɹ���1��ʾkey���ڲ�ɾ��ɹ�
     */
    public long delete(String key) {
        long begin = System.currentTimeMillis();

        long ret = delete(getJedisPool(), SafeEncoder.encode(key));
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        return ret;

    }

    private long delete(JedisPool jedisPool, byte[]... key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    long ret = jedis.del(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0;
    }
    /********************************above:delete,multidelete*****************************/

    /*******************************below:inc******************************************/
    /**
     * ��key������1��<br/>
     * ���key�������ڣ�����ִ�д˲���ǰ����Ĭ��ֵΪ0<br/>
     * ���key���ڣ���key��ʮ������ֵ���ַ��ʾ(e.g: "-123")������Щ��ݻ�������(e.g:return -122L)<br/>
     * ���key���ڣ���key����ʮ������ֵ���ַ��ʾ���򷵻�null
     *
     * @param key
     * @return �������Longֵ��������ʧ�ܷ���null
     */
    public Long incr(String key) {
        return incrBy(key, 1);
    }

    /**
     * ������incr(key).�˷�����������������
     *
     * @param key
     * @param step
     * @return �������Longֵ.������ʧ�ܷ���null
     */
    public Long incrBy(String key, long step) {
        long begin = System.currentTimeMillis();

        Long ret = incrBy(getJedisPool(), SafeEncoder.encode(key), step);
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        return ret;

    }

    private Long incrBy(JedisPool jedisPool, byte[] key, long step) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.incrBy(key, step);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * ȡ�ü�¼incr��desr�ĵ�ǰֵ
     *
     * @param key
     * @return null��ʾ������, ��ֵ��ʾ��key�ĵ�ǰֵ
     */
    public Long getNumberRecordIncrOrDesr(String key) {
        String ret = getString(key);
        if (ret != null) {
            try {
                long num = Long.parseLong(ret);
                return num;
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }
    /********************************above:inc********************************************/

    /*******************************below:desr********************************************/
    /**
     * ��key���Լ�1��<br/>
     * ���key�������ڣ�����ִ�д˲���ǰ����Ĭ��ֵΪ0
     * ���key���ڣ���key��ʮ������ֵ���ַ��ʾ(e.g: "-123")������Щ��ݻ����Լ�(e.g:return -124L)<br/>
     * ���key���ڣ���key����ʮ������ֵ���ַ��ʾ���򷵻�null
     *
     * @param key
     * @return �Լ����Longֵ����Ϊ����������ʧ�ܷ���null
     */
    public Long decr(String key) {
        return decrBy(key, 1);
    }

    /**
     * ������decr(key).�˷����������Լ�����
     *
     * @param key
     * @param step
     * @return �Լ����Longֵ.������ʧ�ܷ���null
     */
    public Long decrBy(String key, long step) {

        return decrBy(getJedisPool(), SafeEncoder.encode(key), step);

    }

    private Long decrBy(JedisPool jedisPool, byte[] key, long step) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.decrBy(key, step);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /********************************above:desc********************************************/

    /*******************************below:append***************************************/
    /**
     * ��ԭvalue��������׷��
     *
     * @param key
     * @param appendStr
     * @return ׷�Ӻ��value���ֽ���
     */
    public Long append(String key, String appendStr) {
        return append(key, SafeEncoder.encode(appendStr));
    }

    /**
     * ��ԭvalue��������׷��
     *
     * @param key
     * @param appendBytes
     * @return ׷�Ӻ��value���ֽ���
     */
    public Long append(String key, byte[] appendBytes) {
        valueTypeAssert(appendBytes);

        return append(getJedisPool(), SafeEncoder.encode(key), appendBytes);

    }

    private Long append(JedisPool jedisPool, byte[] key, byte[] appendBytes) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.append(key, appendBytes);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /********************************above:append***************************************/

    /********************************below:getSet***************************************/
    /**
     * set a new str-value, and return the old str-value
     *
     * @param key
     * @param expireSecond
     * @param value
     * @return
     */
    public String getSetString(String key, int expireSecond, String value) {
        long begin = System.currentTimeMillis();
        valueTypeAssert(value);

        byte[] ret = getSet(getJedisPool(), SafeEncoder.encode(key), expireSecond, SafeEncoder.encode(value));
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        if (ret != null) return SafeEncoder.encode(ret);
        return null;
    }

    /**
     * set a new Object-value, and return a old Object-value
     *
     * @param key
     * @param expireSecond
     * @param value
     * @return
     */
    public Object getSetObject(String key, int expireSecond, Object value) {
        long begin = System.currentTimeMillis();
        valueTypeAssert(value);

        byte[] ret = getSet(getJedisPool(), SafeEncoder.encode(key), expireSecond, serialize(value));
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        if (ret != null) return deserialize(ret);

        return null;
    }

    /**
     * set a new [B-value, and return a old [B-value
     *
     * @param key
     * @param expireSecond
     * @param value
     * @return
     */
    public byte[] getSetByteArr(String key, int expireSecond, byte[] value) {
        long begin = System.currentTimeMillis();
        valueTypeAssert(value);

        byte[] ret = getSet(getJedisPool(), SafeEncoder.encode(key), expireSecond, value);
        if (System.currentTimeMillis() - begin > limitMillSec2log)
            log.error("redis timeout " + (System.currentTimeMillis() - begin) + " ms,key=" + key);
        return ret;
    }

    private byte[] getSet(JedisPool jedisPool, byte[] key, int expireSecond, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    byte[] ret = jedis.getSet(key, value);
                    if (expireSecond > 0) jedis.expire(key, expireSecond);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /********************************above:getSet***************************************/

    /*******************************below:hash hSet************************************/
    /**
     * ���ӻ����hashmap��ĳһ����String���ͣ���<br/>�������ĳ���������ù���ʱ�䣬�ɶ����hashmapʹ��expire(key,time)���ù���ʱ��
     *
     * @param key
     * @param field
     * @param value
     * @return 0 �ɹ���������1�ɹ�������-1����ʧ��
     */
    public long hSetString(String key, String field, String value) {
        valueTypeAssert(value);

        return hSet(getJedisPool(), SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value));

    }

    /**
     * ���ӻ����hashmap��ĳһ����Object����<br/>�������ĳ���������ù���ʱ�䣬�ɶ����hashmapʹ��expire(key,time)���ù���ʱ��
     *
     * @param key
     * @param field
     * @param value
     * @return 0 �ɹ���������1�ɹ�������-1����ʧ��
     */
    public long hSetObject(String key, String field, Object value) {
        valueTypeAssert(value);

        return hSet(getJedisPool(), SafeEncoder.encode(key), SafeEncoder.encode(field), serialize(value));

    }

    /**
     * ���ӻ����hashmap��ĳһ����byte[]��ݿ飩��<br/>�������ĳ���������ù���ʱ�䣬�ɶ����hashmapʹ��expire(key,time)���ù���ʱ��
     *
     * @param key
     * @param field
     * @param value
     * @return 0 �ɹ���������1�ɹ�������-1����ʧ��
     */
    public long hSetByteArr(String key, String field, byte[] value) {
        valueTypeAssert(value);

        return hSet(getJedisPool(), SafeEncoder.encode(key), SafeEncoder.encode(field), value);

    }

    private long hSet(JedisPool jedisPool, byte[] key, byte[] field, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.hset(key, field, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return -1L;
    }
    /********************************above:hash hSet***************************************/

    /*******************************below:hash hMultiSet***************************************/
    /**
     * ͬʱ����ĳhashmap�µ����ɸ�����String����<br/>�������ĳ���������ù���ʱ�䣬�ɶ����hashmapʹ��expire(key,time)���ù���ʱ��
     *
     * @param key
     * @param fieldValues
     * @return "OK" ���³ɹ�����failed��ʧ��
     */
    public String hMultiSetString(String key, Map<String, String> fieldValues) {
        Map<byte[], byte[]> fieldbValuebMap = new HashMap<byte[], byte[]>();
        for (String field : fieldValues.keySet()) {
            String value = fieldValues.get(field);
            if (value != null) fieldbValuebMap.put(SafeEncoder.encode(field), SafeEncoder.encode(value));
        }
        return hMultiSet(key, fieldbValuebMap);
    }

    /**
     * ͬʱ����ĳhashmap�µ����ɸ�����Object����<br/>�������ĳ���������ù���ʱ�䣬�ɶ����hashmapʹ��expire(key,time)���ù���ʱ��
     *
     * @param key
     * @param fieldValues
     * @return "OK" ���³ɹ�����failed��ʧ��
     */
    public String hMultiSetObject(String key, Map<String, Object> fieldValues) {
        Map<byte[], byte[]> fieldbValuebMap = new HashMap<byte[], byte[]>();
        for (String field : fieldValues.keySet()) {
            Object value = fieldValues.get(field);
            if (value != null) fieldbValuebMap.put(SafeEncoder.encode(field), serialize(value));
        }
        return hMultiSet(key, fieldbValuebMap);
    }

    /**
     * ͬʱ����ĳhasmap�µ����ɸ�����byte[]��ݿ飩��<br/>�������ĳ���������ù���ʱ�䣬�ɶ����hashmapʹ��expire(key,time)���ù���ʱ��
     *
     * @param key
     * @param fieldValues
     * @return "OK" ���³ɹ�����failed��ʧ��
     */
    public String hMultiSetByteArr(String key, Map<String, byte[]> fieldValues) {
        Map<byte[], byte[]> fieldbValuebMap = new HashMap<byte[], byte[]>();
        for (String field : fieldValues.keySet()) {
            byte[] value = fieldValues.get(field);
            if (value != null) fieldbValuebMap.put(SafeEncoder.encode(field), value);
        }
        return hMultiSet(key, fieldbValuebMap);
    }

    private String hMultiSet(String key, Map<byte[], byte[]> fieldbValuebMap) {

        return hMultiSet(getJedisPool(), SafeEncoder.encode(key), fieldbValuebMap);
    }

    private String hMultiSet(JedisPool jedisPool, byte[] key, Map<byte[], byte[]> hash) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    String ret = jedis.hmset(key, hash);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return "failed";
    }
    /********************************above:hash hMultiSet***************************************/

    /*******************************below:hash hGet***************************************/
    /**
     * ���ַ�ʽȡ��ĳhashmap�µ�ĳ�����
     *
     * @param key
     * @param field
     * @return
     */
    public String hGetString(String key, String field) {
        byte[] ret = hGetByteArr(key, field);
        if (ret != null) return SafeEncoder.encode(ret);
        return null;
    }

    /**
     * ��Object��ʽȡ��ĳhashmap�µ�ĳ�����
     *
     * @param key
     * @param field
     * @return
     */
    public Object hGetObject(String key, String field) {
        byte[] ret = hGetByteArr(key, field);
        if (ret != null) return deserialize(ret);
        return null;
    }

    /**
     * ����ݿ�ķ�ʽ ȡ��ĳhashmap�µ�ĳ�����
     *
     * @param key
     * @param field
     * @return
     */
    public byte[] hGetByteArr(String key, String field) {

        return hGet(getJedisPool(), SafeEncoder.encode(key), SafeEncoder.encode(field));

    }

    private byte[] hGet(JedisPool jedisPool, byte[] key, byte[] field) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    byte[] ret = jedis.hget(key, field);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /********************************above:hash hGet***************************************/

    /*******************************below:hash hGetAll***************************************/
    /**
     * ���ַ�ķ�ʽ ȡ��ĳhashmap�µ����������
     *
     * @param key
     * @return
     */
    public Map<String, String> hGetAllString(String key) {
        Map<String, byte[]> temp = hGetAllByteArr(key);
        if (temp != null) {
            Map<String, String> ret = new HashMap<String, String>();
            for (String field : temp.keySet()) {
                byte[] value = temp.get(field);
                if (value != null) ret.put(field, SafeEncoder.encode(temp.get(field)));
            }
            return ret;
        }
        return null;
    }

    /**
     * ��Object�ķ�ʽ ȡ��ĳhashmap�µ����������
     *
     * @param key
     * @return
     */
    public Map<String, Object> hGetAllObject(String key) {
        Map<String, byte[]> temp = hGetAllByteArr(key);
        if (temp != null) {
            Map<String, Object> ret = new HashMap<String, Object>();
            for (String field : temp.keySet()) {
                byte[] value = temp.get(field);
                if (value != null) ret.put(field, deserialize(temp.get(field)));
            }
            return ret;
        }
        return null;
    }

    /**
     * ����ݿ�ķ�ʽ ȡ��ĳhashmap�µ����������
     *
     * @param key
     * @return
     */
    public Map<String, byte[]> hGetAllByteArr(String key) {

        return hGetAll(getJedisPool(), SafeEncoder.encode(key));

    }

    private Map<String, byte[]> hGetAll(JedisPool jedisPool, byte[] key) {
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Map<byte[], byte[]> ret = jedis.hgetAll(key);
                    for (Map.Entry<byte[], byte[]> item : ret.entrySet()) {
                        map.put(SafeEncoder.encode(item.getKey()), item.getValue());
                    }
                    return map;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /********************************above:hash hGetAll***************************************/

    /*******************************below:hash hMultiGet**************************************/
    /**
     * ���ַ�ķ�ʽ ȡ��ĳhashmap�µĶ�������
     *
     * @param key
     * @param fields
     * @return
     */
    public Map<String, String> hMultiGetString(String key, List<String> fields) {
        Map<String, byte[]> temp = hMultiGetByteArr(key, fields);
        if (temp != null) {
            Map<String, String> ret = new HashMap<String, String>();
            for (String field : temp.keySet()) {
                byte[] value = temp.get(field);
                if (value != null) ret.put(field, SafeEncoder.encode(temp.get(field)));
            }
            return ret;
        }
        return null;
    }

    /**
     * ��Object�ķ�ʽ ȡ��ĳhashmap�µĶ�������
     *
     * @param key
     * @param fields
     * @return
     */
    public Map<String, Object> hMultiGetObject(String key, List<String> fields) {
        Map<String, byte[]> temp = hMultiGetByteArr(key, fields);
        if (temp != null) {
            Map<String, Object> ret = new HashMap<String, Object>();
            for (String field : temp.keySet()) {
                byte[] value = temp.get(field);
                if (value != null) ret.put(field, deserialize(temp.get(field)));
            }
            return ret;
        }
        return null;
    }

    /**
     * ��byte[]��ݿ�ķ�ʽ ȡ��ĳhashmap�µĶ�������
     *
     * @param key
     * @param fields
     * @return
     */
    public Map<String, byte[]> hMultiGetByteArr(String key, List<String> fields) {

        return hMultiGet(getJedisPool(), SafeEncoder.encode(key), getBArrArr(fields));

    }

    private Map<String, byte[]> hMultiGet(JedisPool jedisPool, byte[] key, byte[]... fields) {
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    List<byte[]> ret = jedis.hmget(key, fields);
                    for (int i = ret.size() - 1; i >= 0; i--) {
                        map.put(SafeEncoder.encode(fields[i]), ret.get(i));
                    }
                    return map;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return map;
    }
    /********************************above:hash hMultiGet***************************************/

    /*******************************below:hash hSetIfNotExist***************************************/
    /**
     * ������hSetString����ֻ����field������ʱ������
     *
     * @param key
     * @param field
     * @param value
     * @return -1����ʧ�ܣ�0 field�Ѵ��ڣ��������ò��ɹ�; 1 field�����ڣ����óɹ�
     */
    public long hSetStringIfNotExist(String key, String field, String value) {
        return hSetByteArrIfNotExist(key, field, SafeEncoder.encode(value));
    }

    /**
     * ������hSetObject����ֻ����field������ʱ������
     *
     * @param key
     * @param field
     * @param value
     * @return -1����ʧ�ܣ�0 field�Ѵ��ڣ��������ò��ɹ�; 1 field�����ڣ����óɹ�
     */
    public long hSetObjectIfNotExist(String key, String field, Object value) {
        return hSetByteArrIfNotExist(key, field, serialize(value));
    }

    /**
     * ������hSetByteArr����ֻ����field������ʱ������
     *
     * @param key
     * @param field
     * @param value
     * @return -1����ʧ�ܣ�0 field�Ѵ��ڣ��������ò��ɹ�; 1 field�����ڣ����óɹ�
     */
    public long hSetByteArrIfNotExist(String key, String field, byte[] value) {
        valueTypeAssert(value);

        return hSetIfNotExist(getJedisPool(), SafeEncoder.encode(key), SafeEncoder.encode(field), value);

    }

    private long hSetIfNotExist(JedisPool jedisPool, byte[] key, byte[] field, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.hsetnx(key, field, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return -1L;
    }
    /********************************above:hash setInNotExist***************************************/

    /*******************************below:hash  hLen,hKeys,hDel,hIncrBy,hExists************************/
    /**
     * ɾ��ĳhashmap��ĳ��
     *
     * @param key
     * @param field
     * @return 1 ɾ��ɹ���0 field �����ڣ�ɾ��ɹ�;-1������쳣
     */
    public long hDelete(String key, String field) {

        return hDelete(getJedisPool(), SafeEncoder.encode(key), SafeEncoder.encode(field));

    }

    private long hDelete(JedisPool jedisPool, byte[] key, byte[] field) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    long ret = jedis.hdel(key, field);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return -1;
    }

    /**
     * ��ȡĳhashmap�е�items��
     *
     * @param key
     * @return
     */
    public long hLen(String key) {

        return hLen(getJedisPool(), SafeEncoder.encode(key));

    }

    private long hLen(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    long ret = jedis.hlen(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0;
    }

    /**
     * ��ȡĳhashmap�µ�����key
     *
     * @param key
     * @return
     */
    public Set<String> hKeys(String key) {

        return hKeys(getJedisPool(), key);

    }

    private Set<String> hKeys(JedisPool jedisPool, String key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Set<String> ret = jedis.hkeys(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * ��hashmap�µ�ĳfield����������������������ɼ�incr()�ӿ�
     *
     * @param key
     * @param field
     * @param step  ������������Ϊ����
     * @return ���������ֵ�����ԭֵΪʮ���Ƶ��ַ��ʾ���򷵻�null
     */
    public Long hIncrBy(String key, String field, long step) {

        return hIncrBy(getJedisPool(), SafeEncoder.encode(key), SafeEncoder.encode(field), step);

    }

    private Long hIncrBy(JedisPool jedisPool, byte[] key, byte[] field, long step) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.hincrBy(key, field, step);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * �ж�ĳhashmap���Ƿ��ĳfield
     *
     * @param key
     * @param field
     * @return
     */
    public Boolean hExists(String key, String field) {

        return hExists(getJedisPool(), SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    private Boolean hExists(JedisPool jedisPool, byte[] key, byte[] field) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Boolean ret = jedis.hexists(key, field);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return false;
    }
    /********************************above:hash  hLen,hKeys,hDel,hIncrBy,hExists************************/

    /******************below:list  lpush,rpush,lpushIfListExist,rpushIfListExist***********************/
    /**
     * ��list�������String���͵�item
     *
     * @param key
     * @param item
     * @return ������ɺ��list����
     */
    public Long lpushString(String key, String item) {
        return lpushByteArr(key, SafeEncoder.encode(item));
    }

    /**
     * ��list�������Object���͵�item
     *
     * @param key
     * @param item
     * @return ������ɺ��list����
     */
    public Long lpushObject(String key, Object item) {
        return lpushByteArr(key, serialize(item));
    }

    /**
     * ��list�������byte[]���͵�item
     *
     * @param key
     * @param item
     * @return ������ɺ��list����
     */
    public Long lpushByteArr(String key, byte[] item) {
        valueTypeAssert(item);

        return lpush(getJedisPool(), SafeEncoder.encode(key), item);
    }

    private Long lpush(JedisPool jedisPool, byte[] key, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.lpush(key, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    /**
     * ��list�Ҷ�����String���͵�item
     *
     * @param key
     * @param item
     * @return ������ɺ��list����
     */
    public Long rpushString(String key, String item) {
        return rpushByteArr(key, SafeEncoder.encode(item));
    }

    /**
     * ��list�Ҷ�����Object���͵�item
     *
     * @param key
     * @param item
     * @return ������ɺ��list����
     */
    public Long rpushObject(String key, Object item) {
        return rpushByteArr(key, serialize(item));
    }

    /**
     * ��list�Ҷ�����byte[]���͵�item
     *
     * @param key
     * @param item
     * @return ������ɺ��list����
     */
    public Long rpushByteArr(String key, byte[] item) {
        valueTypeAssert(item);

        return rpush(getJedisPool(), SafeEncoder.encode(key), item);
    }

    private Long rpush(JedisPool jedisPool, byte[] key, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.rpush(key, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    public Long lpushStringIfListExist(String key, String item) {
        return lpushByteArrIfListExist(key, SafeEncoder.encode(item));
    }

    public Long lpushObjectIfListExist(String key, Object item) {
        return lpushByteArrIfListExist(key, serialize(item));
    }

    public Long lpushByteArrIfListExist(String key, byte[] item) {
        valueTypeAssert(item);

        return lpushIfListExist(getJedisPool(), SafeEncoder.encode(key), item);
    }

    private Long lpushIfListExist(JedisPool jedisPool, byte[] key, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.lpushx(key, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    public Long rpushStringIfListExist(String key, String item) {
        return rpushByteArrIfListExist(key, SafeEncoder.encode(item));
    }

    public Long rpushObjectIfListExist(String key, Object item) {
        return rpushByteArrIfListExist(key, serialize(item));
    }

    public Long rpushByteArrIfListExist(String key, byte[] item) {
        valueTypeAssert(item);

        return rpushIfListExist(getJedisPool(), SafeEncoder.encode(key), item);

    }

    private Long rpushIfListExist(JedisPool jedisPool, byte[] key, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.rpushx(key, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }
    /********************above:list  lpush,rpush,lpushIfListExist,rpushIfListExist***********************/

    /*******************************below:list  lpop,rpop***************************************/
    /**
     * ��list���������String�ķ�ʽ��ȡ����ɾ��һ��item
     *
     * @param key
     * @return
     */
    public String lpopString(String key) {
        byte[] ret = lpopByteArr(key);
        if (ret != null) return SafeEncoder.encode(ret);
        return null;
    }

    /**
     * ��list���������Object�ķ�ʽ��ȡ����ɾ��һ��item
     *
     * @param key
     * @return
     */
    public Object lpopObject(String key) {
        byte[] ret = lpopByteArr(key);
        if (ret != null) return deserialize(ret);
        return null;
    }

    /**
     * ��list���������byte[]�ķ�ʽ��ȡ����ɾ��һ��item
     *
     * @param key
     * @return
     */
    public byte[] lpopByteArr(String key) {

        return lpop(getJedisPool(), SafeEncoder.encode(key));
    }

    private byte[] lpop(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    byte[] ret = jedis.lpop(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * ��list�����Ҷ���String�ķ�ʽ��ȡ����ɾ��һ��item
     *
     * @param key
     * @return
     */
    public String rpopString(String key) {
        byte[] ret = rpopByteArr(key);
        if (ret != null) return SafeEncoder.encode(ret);
        return null;
    }

    /**
     * ��list�����Ҷ���Object�ķ�ʽ��ȡ����ɾ��һ��item
     *
     * @param key
     * @return
     */
    public Object rpopObject(String key) {
        byte[] ret = rpopByteArr(key);
        if (ret != null) return deserialize(ret);
        return null;
    }

    /**
     * ��list�����Ҷ���byte[]�ķ�ʽ��ȡ����ɾ��һ��item
     *
     * @param key
     * @return
     */
    public byte[] rpopByteArr(String key) {

        return rpop(getJedisPool(), SafeEncoder.encode(key));
    }

    private byte[] rpop(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    byte[] ret = jedis.rpop(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /********************************above:list  lpop,rpop***************************************/

    /*******************************below:list  lindex,lrange*************************************/
    /**
     * ��String�ķ�ʽȡ��list��index�������Ҵ�0��ʼ����λ��item
     *
     * @param key
     * @return
     */
    public String lindexString(String key, int index) {
        byte[] ret = lindexByteArr(key, index);
        if (ret != null) return SafeEncoder.encode(ret);
        return null;
    }

    /**
     * ��Object�ķ�ʽȡ��list��index�������Ҵ�0��ʼ����λ��item
     *
     * @param key
     * @return
     */
    public Object lindexObject(String key, int index) {
        byte[] ret = lindexByteArr(key, index);
        if (ret != null) return deserialize(ret);
        return null;
    }

    /**
     * ��byte[]�ķ�ʽȡ��list��index�������Ҵ�0��ʼ����λ��item
     *
     * @param key
     * @return
     */
    public byte[] lindexByteArr(String key, int index) {

        return lindex(getJedisPool(), SafeEncoder.encode(key), index);
    }

    private byte[] lindex(JedisPool jedisPool, byte[] key, int index) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    byte[] ret = jedis.lindex(key, index);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * ��String�ķ�ʽȡ��list��ĳλ������ϵ�items��
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public List<String> lrangeString(String key, int start, int end) {
        List<byte[]> ret = lrangeByteArr(key, start, end);
        if (ret != null) {
            List<String> trueRet = new ArrayList<String>();
            for (byte[] item : ret) {
                if (item != null) trueRet.add(SafeEncoder.encode(item));
            }
            return trueRet;
        }
        return null;
    }

    /**
     * ��Object�ķ�ʽȡ��list��ĳλ������ϵ�items
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public List<Object> lrangeObject(String key, int start, int end) {
        List<byte[]> ret = lrangeByteArr(key, start, end);
        if (ret != null) {
            List<Object> trueRet = new ArrayList<Object>();
            for (byte[] item : ret) {
                if (item != null) trueRet.add(deserialize(item));
            }
            return trueRet;
        }
        return null;
    }

    /**
     * ��byte[]�ķ�ʽȡ��list��ĳλ������ϵ�items
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public List<byte[]> lrangeByteArr(String key, int start, int end) {

        return lrange(getJedisPool(), SafeEncoder.encode(key), start, end);
    }

    private List<byte[]> lrange(JedisPool jedisPool, byte[] key, int start, int end) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    List<byte[]> ret = jedis.lrange(key, start, end);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /********************************above:list  lindex,lrange***************************************/

    /*******************************below:list lset,ltrim***************************************/
    /**
     * ����list��indexλ���ϵ�ֵ��String���ͣ�
     *
     * @param key
     * @param index
     * @param value
     * @return ���index����list���ȷ���failed;�������óɹ�����OK
     */
    public String lsetString(String key, int index, String value) {
        return lsetByteArr(key, index, SafeEncoder.encode(value));
    }

    /**
     * ����list��indexλ���ϵ�ֵ��Object���ͣ�
     *
     * @param key
     * @param index
     * @param value
     * @return ���index����list���ȷ���failed;�������óɹ�����OK
     */
    public String lsetObject(String key, int index, Object value) {
        return lsetByteArr(key, index, serialize(value));
    }

    /**
     * ����list��indexλ���ϵ�ֵ��byte[]���ͣ�
     *
     * @param key
     * @param index
     * @param value
     * @return ���index����list���ȷ���failed;�������óɹ�����OK
     */
    public String lsetByteArr(String key, int index, byte[] value) {
        valueTypeAssert(value);

        return lset(getJedisPool(), SafeEncoder.encode(key), index, value);
    }

    private String lset(JedisPool jedisPool, byte[] key, int index, byte[] value) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    String ret = jedis.lset(key, index, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return "failed";
    }

    /**
     * ��list�ڷ���˽��н�ȡ����Χ֮�ⲿ�ֽ�����������ö���
     *
     * @param key
     * @param start
     * @param end
     * @return ��ȡ�ɹ�����OK�����ָ����Χ����list��ʵ�ʷ�Χ������failed
     */
    public String ltrim(String key, int start, int end) {

        return ltrim(getJedisPool(), SafeEncoder.encode(key), start, end);
    }

    private String ltrim(JedisPool jedisPool, byte[] key, int start, int end) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    String ret = jedis.ltrim(key, start, end);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return "failed";
    }
    /********************************above:list lset,ltrim***************************************/

    /*******************************below:list len***************************************/
    /**
     * ��ȡĳlist�ĳ���
     *
     * @param key
     * @return
     */
    public Long llen(String key) {

        return llen(getJedisPool(), SafeEncoder.encode(key));
    }

    private Long llen(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.llen(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    /**
     *  delete from list
     *
     * @param key
     * @param count
     * @param value
     * @return
     */
    public Long lremString(String key, int count, String value) {
        return lremByteArr(key, count, SafeEncoder.encode(value));
    }

    public Long lremObject(String key, int count, Object value) {
        return lremByteArr(key, count, serialize(value));
    }

    public Long lremByteArr(String key, int count, byte[] value) {

        return lrem(getJedisPool(), SafeEncoder.encode(key), count, value);
    }


    private Long lrem(JedisPool jedisPool, byte[] key, int count, byte[] value) {
        valueTypeAssert(value);
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.lrem(key, count, value);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }
    /********************************above:list len***************************************/

    /*******************************below:set  sadd,srem,spop,smember,srandmember**********************/
    /**
     * ����в���member
     *
     * @param key
     * @param member
     * @return Integer reply, specifically: 1 if the new element was added 0 if
     *         the element was already a member of the set
     */
    public Long saddString(String key, String member) {
        return saddByteArr(key, SafeEncoder.encode(member));
    }

    /**
     * ����в���member(���Ӷ����Ƿ���Ӧ�ڴ˷����д������֤)
     *
     * @param key
     * @param member
     * @return Integer reply, specifically: 1 if the new element was added 0 if
     *         the element was already a member of the set
     */
    public Long saddObject(String key, Object member) {
        return saddByteArr(key, serialize(member));
    }

    /**
     * ����в���member
     *
     * @param key
     * @param member
     * @return Integer reply, specifically: 1 if the new element was added 0 if
     *         the element was already a member of the set
     */
    public Long saddByteArr(String key, byte[] member) {
        valueTypeAssert(member);

        return sadd(getJedisPool(), SafeEncoder.encode(key), member);
    }

    private Long sadd(JedisPool jedisPool, byte[] key, byte[] member) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.sadd(key, member);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    /**
     * ɾ����е�member��Ա
     *
     * @param key
     * @param member
     * @return Integer reply, specifically: 1 if the new element was removed 0
     *         if the new element was not a member of the set
     */
    public Long sremString(String key, String member) {
        return sremByteArr(key, SafeEncoder.encode(member));
    }

    /**
     * ɾ����е�member��Ա�����Ӷ����Ƿ��ʺϴ˷����д���ԣ�
     *
     * @param key
     * @param member
     * @return Integer reply, specifically: 1 if the new element was removed 0
     *         if the new element was not a member of the set
     */
    public Long sremObject(String key, Object member) {
        return sremByteArr(key, serialize(member));
    }

    /**
     * ɾ����е�member��Ա
     *
     * @param key
     * @param member
     * @return Integer reply, specifically: 1 if the new element was removed 0
     *         if the new element was not a member of the set
     */
    public Long sremByteArr(String key, byte[] member) {
        valueTypeAssert(member);

        return srem(getJedisPool(), SafeEncoder.encode(key), member);

    }

    private Long srem(JedisPool jedisPool, byte[] key, byte[] member) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.srem(key, member);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    /**
     * Remove and return a random member from a set
     *
     * @param key
     * @return
     */
    public String spopString(String key) {
        byte[] ret = spopByteArr(key);
        if (ret != null) return SafeEncoder.encode(ret);
        return null;
    }

    /**
     * Remove and return a random member from a set
     *
     * @param key
     * @return
     */
    public Object spopObject(String key) {
        byte[] ret = spopByteArr(key);
        if (ret != null) return deserialize(ret);
        return null;
    }

    /**
     * Remove and return a random member from a set
     *
     * @param key
     * @return
     */
    public byte[] spopByteArr(String key) {

        return spop(getJedisPool(), SafeEncoder.encode(key));
    }

    private byte[] spop(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    byte[] ret = jedis.spop(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * Get all the members in a set
     *
     * @param key
     * @return
     */
    public Set<String> smemberString(String key) {
        Set<byte[]> ret = smemberByteArr(key);
        if (ret != null) {
            Set<String> trueRet = new HashSet<String>();
            for (byte[] member : ret) {
                if (member != null) trueRet.add(SafeEncoder.encode(member));
            }
            return trueRet;
        }
        return null;
    }

    /**
     * Get all the members in a set
     *
     * @param key
     * @return
     */
    public Set<Object> smemberObject(String key) {
        Set<byte[]> ret = smemberByteArr(key);
        if (ret != null) {
            Set<Object> trueRet = new HashSet<Object>();
            for (byte[] member : ret) {
                if (member != null) trueRet.add(deserialize(member));
            }
            return trueRet;
        }
        return null;
    }

    /**
     * Get all the members in a set
     *
     * @param key
     * @return
     */
    public Set<byte[]> smemberByteArr(String key) {

        return smember(getJedisPool(), SafeEncoder.encode(key));

    }

    private Set<byte[]> smember(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Set<byte[]> ret = jedis.smembers(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * Get a rand member in a set
     *
     * @param key
     * @return
     */
    public String srandmemberString(String key) {

        byte[] ret = srandmember(getJedisPool(), SafeEncoder.encode(key));
        if (ret != null) return SafeEncoder.encode(ret);
        return null;
    }

    /**
     * Get a rand member in a set
     *
     * @param key
     * @return
     */
    public Object srandmemberObject(String key) {

        byte[] ret = srandmember(getJedisPool(), SafeEncoder.encode(key));
        if (ret != null) return deserialize(ret);
        return null;
    }

    /**
     * Get a rand member in a set
     *
     * @param key
     * @return
     */
    public byte[] srandmemberByteArr(String key) {

        return srandmember(getJedisPool(), SafeEncoder.encode(key));

    }

    private byte[] srandmember(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    byte[] ret = jedis.srandmember(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /********************************above:set  sadd,srem,spop,smember,srandmember*************************/

    /*******************************below:set  scard,sismember*******************************/
    /**
     * �ж�ĳ��member�Ƿ��ڴ˼�����
     *
     * @param key
     * @param member
     * @return
     */
    public Boolean sismemberString(String key, String member) {
        return sismemberByteArr(key, SafeEncoder.encode(member));
    }

    /**
     * �ж�ĳ��member�Ƿ��ڴ˼�����
     *
     * @param key
     * @param member
     * @return
     */
    public Boolean sismemberObject(String key, Object member) {
        return sismemberByteArr(key, serialize(member));
    }

    /**
     * �ж�ĳ��member�Ƿ��ڴ˼�����
     *
     * @param key
     * @param member
     * @return
     */
    public Boolean sismemberByteArr(String key, byte[] member) {
        if (member == null) return false;

        return sismember(getJedisPool(), SafeEncoder.encode(key), member);

    }

    private Boolean sismember(JedisPool jedisPool, byte[] key, byte[] member) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Boolean ret = jedis.sismember(key, member);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return false;
    }

    /**
     * Get the number of members in a set
     *
     * @param key
     * @return
     */
    public Long scard(String key) {

        return scard(getJedisPool(), SafeEncoder.encode(key));

    }

    private Long scard(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.scard(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }
    /********************************above:set  scard,sismember*******************************/

    /******************************below: set  sinter��sinterstore *************************/
    /**
     * �������ϵĺϼ����ٷ���sinter���������ϵĽ�������ǰ�ڰ汾ʵ��Ϊ�ϼ�����Щ�����ʶΪ�ϳ�
     *
     * @param keys
     * @return
     */
    @Deprecated
    public Set<String> sinterString(List<String> keys) {
        valueTypeAssert(keys);
        Set<String> ret = new HashSet<String>();
        for (String key : keys) {
            Set<String> set = smemberString(key);
            ret.addAll(set);
        }
        return ret;
    }

    /**
     * �������ϵĺϼ����ٷ���sinter���������ϵĽ�������ǰ�ڰ汾ʵ��Ϊ�ϼ�����Щ�����ʶΪ�ϳ�
     *
     * @param keys
     * @return
     */
    @Deprecated
    public Set<Object> sinterObject(List<String> keys) {
        valueTypeAssert(keys);
        Map<String, byte[]> retMap = sinter(keys);
        Set<Object> ret = new HashSet<Object>();
        if (retMap != null) {
            for (byte[] item : retMap.values()) ret.add(deserialize(item));
        }
        return ret;
    }

    /**
     * �������ϵĺϼ����ٷ���sinter���������ϵĽ�������ǰ�ڰ汾ʵ��Ϊ�ϼ�����Щ�����ʶΪ�ϳ�
     *
     * @param keys
     * @return
     */
    @Deprecated
    public Set<byte[]> sinterByteArr(List<String> keys) {
        valueTypeAssert(keys);
        Map<String, byte[]> retMap = sinter(keys);
        Set<byte[]> ret = new HashSet<byte[]>();
        if (retMap != null) ret.addAll(retMap.values());
        return ret;
    }

    @Deprecated
    private Map<String, byte[]> sinter(List<String> keys) {
        valueTypeAssert(keys);
        Map<String, byte[]> ret = new HashMap<String, byte[]>();
        for (String key : keys) {
            Set<byte[]> set = smemberByteArr(key);
            if (set != null) {
                for (byte[] item : set) ret.put(SafeEncoder.encode(item), item);
            }
        }
        return ret;
    }

    /******************************below:sorted set  ZADD��zaddMulti*************************/
    /**
     * Add the specified member having the specifeid score to the sorted set
     * stored at key. If member is already a member of the sorted set the score
     * is updated, and the element reinserted in the right position to ensure
     * sorting. If key does not exist a new sorted set with the specified member
     * as sole member is created. If the key exists but does not hold a sorted
     * set value an error is returned.
     * <p/>
     * The score value can be the string representation of a double precision
     * floating point number.
     * <p/>
     * Time complexity O(log(N)) with N being the number of elements in the
     * sorted set
     *
     * @param key
     * @param score
     * @param member
     * @return Integer reply, specifically: 1 if the new element was added;  0 if
     *         the element was already a member of the sorted set and the score
     *         was updated ; -1 the error happened in server
     */
    public Long zaddString(String key, double score, String member) {
        return zaddByteArr(key, score, SafeEncoder.encode(member));
    }

    public Long zaddObject(String key, double score, Object member) {
        return zaddByteArr(key, score, serialize(member));
    }

    public Long zaddByteArr(String key, double score, byte[] member) {
        valueTypeAssert(member);

        return zadd(getJedisPool(), SafeEncoder.encode(key), score, member);
    }

    private Long zadd(JedisPool jedisPool, byte[] key, double score, byte[] member) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.zadd(key, score, member);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    /**
     * ��sorted set�в�����Ԫ��
     *
     * @param key
     * @param scoreMembers
     * @return �ɹ�����Ԫ�صĸ���
     */
    public Long zaddMultiString(String key, Map<Double, String> scoreMembers) {
        Map<Double, byte[]> scoreBMembers = new HashMap<Double, byte[]>();
        for (Map.Entry<Double, String> item : scoreMembers.entrySet()) {
            if (item.getValue() != null) scoreBMembers.put(item.getKey(), SafeEncoder.encode(item.getValue()));
        }
        return zaddMultiByteArr(key, scoreBMembers);
    }

    /**
     * ��sorted set�в�����Ԫ��
     *
     * @param key
     * @param scoreMembers
     * @return �ɹ�����Ԫ�صĸ���
     */
    public Long zaddMultiObject(String key, Map<Double, Object> scoreMembers) {
        Map<Double, byte[]> scoreBMembers = new HashMap<Double, byte[]>();
        for (Map.Entry<Double, Object> item : scoreMembers.entrySet()) {
            if (item.getValue() != null) scoreBMembers.put(item.getKey(), serialize(item.getValue()));
        }
        return zaddMultiByteArr(key, scoreBMembers);
    }

    /**
     * ��sorted set�в�����Ԫ��
     *
     * @param key
     * @param scoreMembers
     * @return �ɹ�����Ԫ�صĸ���
     */
    public Long zaddMultiByteArr(String key, Map<Double, byte[]> scoreMembers) {
        if (scoreMembers == null || scoreMembers.size() == 0) return 0L;

        return zadd(getJedisPool(), SafeEncoder.encode(key), scoreMembers);

    }

    private Long zadd(JedisPool jedisPool, byte[] key, Map<Double, byte[]> scoreMembers) {
        Jedis jedis = null;
//        if (jedisPool != null) {
//            try {
//                jedis = jedisPool.getResource();
//                if (jedis != null) {
//                    Long ret = jedis.zadd(key, scoreMembers);
//                    return ret;
//                }
//            } catch (JedisConnectionException e) {
//                if (jedis != null) jedisPool.returnBrokenResource(jedis);
//                log.error(e.getMessage(), e);
//                jedis = null;
//            } catch (Exception e) {
//                log.error(e.getMessage(), e);
//            } finally {
//                if (jedis != null) jedisPool.returnResource(jedis);
//            }
//        }
        return 0L;
    }


    private Long zremrangeByRank(JedisPool jedisPool, byte[] key, int start, int end) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.zremrangeByRank(key, start, end);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    /**
     * �����򼯺���ɾ��ָ��score��ΧԪ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @return ��ɾ���Ԫ�ظ���
     */
    public Long zremrangeByScore(String key, double minScore, double maxScore) {

        return zremrangeByScore(getJedisPool(), SafeEncoder.encode(key), minScore, maxScore);

    }

    private Long zremrangeByScore(JedisPool jedisPool, byte[] key, double minScore, double maxScore) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.zremrangeByScore(key, minScore, maxScore);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    /**
     * �����򼯺���ɾ��ָ��Ԫ��
     *
     * @param key
     * @param member
     * @return 0��δ�ҵ���Ԫ��ɾ��ɹ��� 1���ҵ���ɾ��ɹ�
     */
    public Long zremString(String key, String member) {
        return zremByteArr(key, SafeEncoder.encode(member));
    }

    /**
     * �����򼯺���ɾ��ָ��Ԫ��
     *
     * @param key
     * @param member
     * @return 0��δ�ҵ���Ԫ��ɾ��ɹ��� 1���ҵ���ɾ��ɹ�
     */
    public Long zremObject(String key, Object member) {
        return zremByteArr(key, serialize(member));
    }

    /**
     * �����򼯺���ɾ��ָ��Ԫ��
     *
     * @param key
     * @param member
     * @return 0��δ�ҵ���Ԫ��ɾ��ɹ��� 1���ҵ���ɾ��ɹ�
     */
    public Long zremByteArr(String key, byte[] member) {
        if (member == null) return 0L;

        return zrem(getJedisPool(), SafeEncoder.encode(key), member);

    }

    /**
     * �����򼯺���ɾ����ָ��Ԫ��
     *
     * @param key
     * @param memberList
     * @return �ɹ�ɾ���Ԫ�ظ���
     */
    public Long zremMultiString(String key, List<String> memberList) {
        List<byte[]> bArrList = new ArrayList<byte[]>();
        for (String member : memberList) {
            if (member != null) bArrList.add(SafeEncoder.encode(member));
        }
        return zremMultiByteArr(key, bArrList);
    }

    /**
     * �����򼯺���ɾ����ָ��Ԫ��
     *
     * @param key
     * @param memberList
     * @return �ɹ�ɾ���Ԫ�ظ���
     */
    public Long zremMultiObject(String key, List<Object> memberList) {
        List<byte[]> bArrList = new ArrayList<byte[]>();
        for (Object member : memberList) {
            if (member != null) bArrList.add(serialize(member));
        }
        return zremMultiByteArr(key, bArrList);
    }

    /**
     * �����򼯺���ɾ����ָ��Ԫ��
     *
     * @param key
     * @param memberList
     * @return �ɹ�ɾ���Ԫ�ظ���
     */
    public Long zremMultiByteArr(String key, List<byte[]> memberList) {
        byte[][] memberB = new byte[memberList.size()][];
        for (int i = memberList.size() - 1; i >= 0; i--) memberB[i] = memberList.get(i);

        return zrem(getJedisPool(), SafeEncoder.encode(key), memberB);

    }

    private Long zrem(JedisPool jedisPool, byte[] key, byte[]... member) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.zrem(key, member);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }
    /******************************above:sorted set  zremrangeByRank��ZREM��zremMulti*************************/

    /**************************below:sorted set  ZCARD��ZCOUNT*********************/
    /**
     * ��ȡsorted set ��Ԫ�ظ���
     *
     * @param key
     * @return
     */
    public Long zcard(String key) {

        return zcard(getJedisPool(), SafeEncoder.encode(key));

    }

    /**
     * ��ȡsorted set ��ָ����Χ�ڵ�Ԫ�ظ���
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @return
     */
    public Long zcount(String key, double minScore, double maxScore) {

        return zcount(getJedisPool(), SafeEncoder.encode(key), minScore, maxScore);

    }

    private Long zcard(JedisPool jedisPool, byte[] key) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.zcard(key);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }

    private Long zcount(JedisPool jedisPool, byte[] key, double minScore, double maxScore) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Long ret = jedis.zcount(key, minScore, maxScore);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return 0L;
    }
    /****************************above:sorted set  ZCARD��ZCOUNT*********************/

    /******************************below:sorted set  ZRANK,ZREVRANK��ZSCORE*************************/
    /**
     * ����Ԫ�������򼯺ϣ���С�����е���ţ���0��ʼ��
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������ţ�Ԫ�ز�����ʱ����null
     */
    public Long zrankString(String key, String member) {
        return zrankByteArr(key, SafeEncoder.encode(member));
    }

    /**
     * ����Ԫ�������򼯺ϣ���С�����е���ţ���0��ʼ��
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������ţ�Ԫ�ز�����ʱ����null
     */
    public Long zrankObject(String key, Object member) {
        return zrankByteArr(key, serialize(member));
    }

    /**
     * ����Ԫ�������򼯺ϣ���С�����е���ţ���0��ʼ��
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������ţ�Ԫ�ز�����ʱ����null
     */
    public Long zrankByteArr(String key, byte[] member) {
        valueTypeAssert(member);

        return zrank(getJedisPool(), SafeEncoder.encode(key), member, true);

    }

    /**
     * ����Ԫ�������򼯺ϣ��Ӵ�С���е���ţ���0��ʼ��
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������ţ�Ԫ�ز�����ʱ����null
     */
    public Long zrevrankString(String key, String member) {
        return zrevrankByteArr(key, SafeEncoder.encode(member));
    }

    /**
     * ����Ԫ�������򼯺ϣ��Ӵ�С���е���ţ���0��ʼ��
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������ţ�Ԫ�ز�����ʱ����null
     */
    public Long zrevrankObject(String key, Object member) {
        return zrevrankByteArr(key, serialize(member));
    }

    /**
     * ����Ԫ�������򼯺ϣ��Ӵ�С���е���ţ���0��ʼ��
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������ţ�Ԫ�ز�����ʱ����null
     */
    public Long zrevrankByteArr(String key, byte[] member) {
        valueTypeAssert(member);

        return zrank(getJedisPool(), SafeEncoder.encode(key), member, false);
    }

    private Long zrank(JedisPool jedisPool, byte[] key, byte[] member, boolean isasc) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    if (isasc) {
                        Long ret = jedis.zrank(key, member);
                        return ret;
                    } else {
                        Long ret = jedis.zrevrank(key, member);
                        return ret;
                    }
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * ����Ԫ�������򼯺��е���������
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������������score��Ԫ�ز�����ʱ����null
     */
    public Double zscoreString(String key, String member) {
        return zscoreByteArr(key, SafeEncoder.encode(member));
    }

    /**
     * ����Ԫ�������򼯺��е���������
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������������score��Ԫ�ز�����ʱ����null
     */
    public Double zscoreObject(String key, Object member) {
        return zscoreByteArr(key, serialize(member));
    }

    /**
     * ����Ԫ�������򼯺��е���������
     *
     * @param key
     * @param member
     * @return ���Ԫ�ش���ʱ������������score��Ԫ�ز�����ʱ����null
     */
    public Double zscoreByteArr(String key, byte[] member) {
        valueTypeAssert(member);

        return zscore(getJedisPool(), SafeEncoder.encode(key), member);

    }

    private Double zscore(JedisPool jedisPool, byte[] key, byte[] member) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    Double ret = jedis.zscore(key, member);
                    return ret;
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /******************************above:sorted set  ZRANK,ZREVRANK��ZSCORE*************************/

    /***************************below:sorted set  ZRANGE��ZREVRANGE*******************/
    /**
     * ��ȡָ��λ�÷�Χ�ڵ����򼯺�
     *
     * @param key
     * @param start
     * @param end
     * @return LinkedHashSet���
     */
    public Set<String> zrangeString(String key, int start, int end) {
        Set<byte[]> ret = zrangeByteArr(key, start, end);
        if (ret != null) {
            Set<String> trueRet = new LinkedHashSet<String>(); //used LinkedHashSet to ensure it is in order
            for (byte[] item : ret) trueRet.add(SafeEncoder.encode(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ��λ�÷�Χ�ڵ����򼯺�
     *
     * @param key
     * @param start
     * @param end
     * @return LinkedHashSet���
     */
    public Set<Object> zrangeObject(String key, int start, int end) {
        Set<byte[]> ret = zrangeByteArr(key, start, end);
        if (ret != null) {
            Set<Object> trueRet = new LinkedHashSet<Object>();
            for (byte[] item : ret) trueRet.add(deserialize(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ��λ�÷�Χ�ڵ����򼯺�
     *
     * @param key
     * @param start
     * @param end
     * @return LinkedHashSet���
     */
    public Set<byte[]> zrangeByteArr(String key, int start, int end) {

        return zrange(getJedisPool(), SafeEncoder.encode(key), start, end, true);

    }

    /**
     * ��ȡָ��λ�÷�Χ�ڵĽ��򼯺�
     *
     * @param key
     * @param start
     * @param end
     * @return LinkedHashSet���
     */
    public Set<String> zrevrangeString(String key, int start, int end) {
        Set<byte[]> ret = zrevrangeByteArr(key, start, end);
        if (ret != null) {
            Set<String> trueRet = new LinkedHashSet<String>();
            for (byte[] item : ret) trueRet.add(SafeEncoder.encode(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ��λ�÷�Χ�ڵĽ��򼯺�
     *
     * @param key
     * @param start
     * @param end
     * @return LinkedHashSet���
     */
    public Set<Object> zrevrangeObject(String key, int start, int end) {
        Set<byte[]> ret = zrevrangeByteArr(key, start, end);
        if (ret != null) {
            Set<Object> trueRet = new LinkedHashSet<Object>();
            for (byte[] item : ret) trueRet.add(deserialize(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ��λ�÷�Χ�ڵĽ��򼯺�
     *
     * @param key
     * @param start
     * @param end
     * @return LinkedHashSet���
     */
    public Set<byte[]> zrevrangeByteArr(String key, int start, int end) {

        return zrange(getJedisPool(), SafeEncoder.encode(key), start, end, false);

    }

    private Set<byte[]> zrange(JedisPool jedisPool, byte[] key, int start, int end, boolean isasc) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    if (isasc) {
                        Set<byte[]> ret = jedis.zrange(key, start, end);
                        return ret;
                    } else {
                        Set<byte[]> ret = jedis.zrevrange(key, start, end);
                        return ret;
                    }
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /***************************above:sorted set  ZRANGE��ZREVRANGE*******************/

    /******************************below:sorted set  zrangeByScore��zrevrangeByScore*************************/
    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @return LinkedHashSet ������
     */
    public Set<String> zrangeByScoreString(String key, double minScore, double maxScore) {
        Set<byte[]> ret = zrangeByScoreByteArr(key, minScore, maxScore);
        if (ret != null) {
            Set<String> trueRet = new LinkedHashSet<String>();
            for (byte[] item : ret) trueRet.add(SafeEncoder.encode(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @return LinkedHashSet������
     */
    public Set<Object> zrangeByScoreObject(String key, double minScore, double maxScore) {
        Set<byte[]> ret = zrangeByScoreByteArr(key, minScore, maxScore);
        if (ret != null) {
            Set<Object> trueRet = new LinkedHashSet<Object>();
            for (byte[] item : ret) trueRet.add(deserialize(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @return LinkedHashSet������
     */
    public Set<byte[]> zrangeByScoreByteArr(String key, double minScore, double maxScore) {

        return zrangeByScore(getJedisPool(), SafeEncoder.encode(key), minScore, maxScore, true);

    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @return LinkedHashSet ������
     */
    public Set<String> zrevrangeByScoreString(String key, double maxScore, double minScore) {
        Set<byte[]> ret = zrevrangeByScoreByteArr(key, maxScore, minScore);
        if (ret != null) {
            Set<String> trueRet = new LinkedHashSet<String>();
            for (byte[] item : ret) trueRet.add(SafeEncoder.encode(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @return LinkedHashSet������
     */
    public Set<Object> zrevrangeByScoreObject(String key, double maxScore, double minScore) {
        Set<byte[]> ret = zrevrangeByScoreByteArr(key, maxScore, minScore);
        if (ret != null) {
            Set<Object> trueRet = new LinkedHashSet<Object>();
            for (byte[] item : ret) trueRet.add(deserialize(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @return LinkedHashSet������
     */
    public Set<byte[]> zrevrangeByScoreByteArr(String key, double maxScore, double minScore) {
        return zrangeByScore(getJedisPool(), SafeEncoder.encode(key), minScore, maxScore, false);
    }

    private Set<byte[]> zrangeByScore(JedisPool jedisPool, byte[] key, double minScore, double maxScore, boolean isasc) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    if (isasc) {
                        Set<byte[]> ret = jedis.zrangeByScore(key, minScore, maxScore);
                        return ret;
                    } else {
                        Set<byte[]> ret = jedis.zrevrangeByScore(key, maxScore, minScore);
                        return ret;
                    }
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @param offset
     * @param count
     * @return LinkedHashSet ������
     */
    public Set<String> zrangeByScoreString(String key, double minScore, double maxScore, int offset, int count) {
        Set<byte[]> ret = zrangeByScoreByteArr(key, minScore, maxScore, offset, count);
        if (ret != null) {
            Set<String> trueRet = new LinkedHashSet<String>();
            for (byte[] item : ret) trueRet.add(SafeEncoder.encode(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @param offset
     * @param count
     * @return LinkedHashSet������
     */
    public Set<Object> zrangeByScoreObject(String key, double minScore, double maxScore, int offset, int count) {
        Set<byte[]> ret = zrangeByScoreByteArr(key, minScore, maxScore, offset, count);
        if (ret != null) {
            Set<Object> trueRet = new LinkedHashSet<Object>();
            for (byte[] item : ret) trueRet.add(deserialize(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @param offset
     * @param count
     * @return LinkedHashSet������
     */
    public Set<byte[]> zrangeByScoreByteArr(String key, double minScore, double maxScore, int offset, int count) {

        return zrangeByScore(getJedisPool(), SafeEncoder.encode(key), minScore, maxScore, offset, count, true);

    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @param offset
     * @param count
     * @return LinkedHashSet ������
     */
    public Set<String> zrevrangeByScoreString(String key, double maxScore, double minScore, int offset, int count) {
        Set<byte[]> ret = zrevrangeByScoreByteArr(key, maxScore, minScore, offset, count);
        if (ret != null) {
            Set<String> trueRet = new LinkedHashSet<String>();
            for (byte[] item : ret) trueRet.add(SafeEncoder.encode(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @param offset
     * @param count
     * @return LinkedHashSet������
     */
    public Set<Object> zrevrangeByScoreObject(String key, double maxScore, double minScore, int offset, int count) {
        Set<byte[]> ret = zrevrangeByScoreByteArr(key, maxScore, minScore, offset, count);
        if (ret != null) {
            Set<Object> trueRet = new LinkedHashSet<Object>();
            for (byte[] item : ret) trueRet.add(deserialize(item));
            return trueRet;
        }
        return null;
    }

    /**
     * ��ȡָ����������score��Χ�ڵ�Ԫ��
     *
     * @param key
     * @param minScore
     * @param maxScore
     * @param offset
     * @param count
     * @return LinkedHashSet������
     */
    public Set<byte[]> zrevrangeByScoreByteArr(String key, double maxScore, double minScore, int offset, int count) {

        return zrangeByScore(getJedisPool(), SafeEncoder.encode(key), minScore, maxScore, offset, count, false);

    }

    private Set<byte[]> zrangeByScore(JedisPool jedisPool, byte[] key, double minScore, double maxScore, int offset, int count, boolean isasc) {
        Jedis jedis = null;
        if (jedisPool != null) {
            try {
                jedis = jedisPool.getResource();
                if (jedis != null) {
                    if (isasc) {
                        Set<byte[]> ret = jedis.zrangeByScore(key, minScore, maxScore, offset, count);
                        return ret;
                    } else {
                        Set<byte[]> ret = jedis.zrevrangeByScore(key, maxScore, minScore, offset, count);
                        return ret;
                    }
                }
            } catch (JedisConnectionException e) {
                if (jedis != null) jedisPool.returnBrokenResource(jedis);
                log.error(e.getMessage(), e);
                jedis = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (jedis != null) jedisPool.returnResource(jedis);
            }
        }
        return null;
    }
    /******************************above:sorted set  zrangeByScore��zrevrangeByScore*************************/
}