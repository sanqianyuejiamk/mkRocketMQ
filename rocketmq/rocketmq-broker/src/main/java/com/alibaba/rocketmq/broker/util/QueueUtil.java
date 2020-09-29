package com.alibaba.rocketmq.broker.util;

import java.util.Random;

/**
 * User: mengka
 * Date: 15-5-17-下午9:31
 */
public class QueueUtil {

    protected static final Random random = new Random(System.currentTimeMillis());

    /**
     *  选择一个随机的queueId，写入数据
     *
     * @param writeQueueNums
     * @return
     */
    public static int getRandomQueueId(int writeQueueNums) {
        int queueIdInt = Math.abs(random.nextInt() % 99999999) % writeQueueNums;
        return queueIdInt;
    }

}
