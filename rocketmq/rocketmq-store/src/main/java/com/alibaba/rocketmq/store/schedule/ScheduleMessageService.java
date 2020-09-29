/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.rocketmq.common.ConfigManager;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.running.RunningStats;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.config.StorePathConfigHelper;


/**
 * 定时消息服务
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class ScheduleMessageService extends ConfigManager {

    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";

    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private static final long FIRST_DELAY_TIME_30min = 30*60*000L;//30 minutes

    // 每个level对应的延时时间
    private final ConcurrentHashMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(32);

    // 延时计算到了哪里
    private final ConcurrentHashMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);

    public static final ExecutorService SCHEDULE_EXECUTOR_SERVICE = Executors
            .newCachedThreadPool();

    // 定时器
    private final Timer timer = new Timer("ScheduleMessageTimerThread", true);

    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;

    // 最大值
    private int maxDelayLevel;


    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQuque(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }


    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }


    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }


    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }


    public void start() {
        int levelCount = this.delayLevelTable.keySet().size();
        log.info(String.format("ScheduleMessageService START size = %s , delayLevelTable = %s , offsetTable = %s",levelCount, JSON.toJSONString(delayLevelTable),JSON.toJSONString(offsetTable)));

        final Semaphore scheduleSemaphore = new Semaphore(levelCount);

        // 为每个延时队列增加定时器
        for (final Integer level : this.delayLevelTable.keySet()) {
            SCHEDULE_EXECUTOR_SERVICE.execute(new Runnable() {
                @Override
                public void run() {
                    Long scheduleOffset = offsetTable.get(level);
                    log.info("DeliverDelayedMessageTimerTask run...!!! level = {} , offset = {}", level, scheduleOffset);
                    timer.schedule(new DeliverDelayedMessageTimerTask(level, scheduleOffset!=null?scheduleOffset:0L, offsetTable, delayLevelTable, defaultMessageStore,scheduleSemaphore),0, FIRST_DELAY_TIME_30min);
                }
            });
        }
        SCHEDULE_EXECUTOR_SERVICE.shutdown();

        // 定时将延时进度刷盘
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    ScheduleMessageService.this.persist();
                } catch (Exception e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
    }


    /**
     *  每个延时队列的处理
     *
     * @param level
     */
    public void deliverDelayedMessage(Integer level){

     }

    public void shutdown() {
        this.timer.cancel();
    }


    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }


    public String encode() {
        return this.encode(false);
    }


    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }


    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }


    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }


    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }


    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }


}
