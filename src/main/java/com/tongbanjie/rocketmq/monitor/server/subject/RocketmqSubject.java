package com.tongbanjie.rocketmq.monitor.server.subject;

import com.tongbanjie.rocketmq.monitor.server.observer.RocketmqObserver;

import java.util.Vector;

/**
 * User: mengka
 * Date: 15-6-13-下午9:19
 */
public class RocketmqSubject {

    private boolean changed = false;

    private Vector obs;

    private RocketmqSubject() {
        obs = new Vector();
    }

    /**
     * 添加观察者
     *
     * @param o
     */
    public synchronized void addObserver(RocketmqObserver o) {
        if (o == null)
            throw new NullPointerException();
        if (!obs.contains(o)) {
            obs.addElement(o);
        }
    }

    public synchronized void deleteObserver(RocketmqObserver o) {
        obs.removeElement(o);
    }

    public void notifyObservers() {
        notifyObservers(null);
    }

    /**
     * 通知所有的观察者
     *
     * @param arg
     */
    public void notifyObservers(Object arg) {
        Object[] arrLocal;
        synchronized (this) {
            if (!changed)
                return;
            arrLocal = obs.toArray();
            clearChanged();
        }
        for (int i = arrLocal.length - 1; i >= 0; i--) {
            ((RocketmqObserver) arrLocal[i]).update(this, arg);
        }
    }

    public synchronized void deleteObservers() {
        obs.removeAllElements();
    }

    public synchronized void setChanged() {
        changed = true;
    }

    protected synchronized void clearChanged() {
        changed = false;
    }

    public synchronized boolean hasChanged() {
        return changed;
    }

    /**
     * * Returns the number of observers of this <tt>Observable</tt> object. * * @return
     * the number of observers of this object.
     */
    public synchronized int countObservers() {
        return obs.size();
    }

    public static RocketmqSubject getInitializer(){
        return RocketmqSubjectHolder.rocketmqSubject_Holder;
    }

    public static class RocketmqSubjectHolder{
        private static RocketmqSubject rocketmqSubject_Holder = new RocketmqSubject();
    }
}
