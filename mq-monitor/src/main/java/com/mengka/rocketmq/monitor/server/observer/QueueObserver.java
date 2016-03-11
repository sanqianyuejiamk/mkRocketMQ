package com.mengka.rocketmq.monitor.server.observer;

import com.mengka.rocketmq.monitor.server.subject.RocketmqSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: mengka
 * Date: 15-6-13-обнГ9:28
 */
public class QueueObserver implements RocketmqObserver {

    private static final Logger log = LoggerFactory.getLogger(QueueObserver.class);

    @Override
    public void update(RocketmqSubject subject, Object arg) {
        log.info("QueueObserver receive subject message = "+arg);

    }
}
