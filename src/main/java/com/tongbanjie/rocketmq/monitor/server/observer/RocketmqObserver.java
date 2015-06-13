package com.tongbanjie.rocketmq.monitor.server.observer;

import com.tongbanjie.rocketmq.monitor.server.subject.RocketmqSubject;

/**
 * User: mengka
 * Date: 15-6-13-обнГ9:21
 */
public interface RocketmqObserver {

    void update(RocketmqSubject subject, Object arg);
}
