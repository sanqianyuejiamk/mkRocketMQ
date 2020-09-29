package com.alibaba.rocketmq.remoting.common;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * 为减少网络传输数量准备
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SendMessageRequestHeaderV2 implements CommandCustomHeader {
    @CFNotNull
    private String a;// producerGroup;
    @CFNotNull
    private String b;// topic;
    @CFNotNull
    private String c;// defaultTopic;
    @CFNotNull
    private Integer d;// defaultTopicQueueNums;
    @CFNotNull
    private Integer e;// queueId;
    @CFNotNull
    private Integer f;// sysFlag;
    @CFNotNull
    private Long g;// bornTimestamp;
    @CFNotNull
    private Integer h;// flag;
    @CFNullable
    private String i;// properties;
    @CFNullable
    private Integer j;// reconsumeTimes;
    @CFNullable
    private boolean k;// unitMode = false;


    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getA() {
        return a;
    }


    public void setA(String a) {
        this.a = a;
    }


    public String getB() {
        return b;
    }


    public void setB(String b) {
        this.b = b;
    }


    public String getC() {
        return c;
    }


    public void setC(String c) {
        this.c = c;
    }


    public Integer getD() {
        return d;
    }


    public void setD(Integer d) {
        this.d = d;
    }


    public Integer getE() {
        return e;
    }


    public void setE(Integer e) {
        this.e = e;
    }


    public Integer getF() {
        return f;
    }


    public void setF(Integer f) {
        this.f = f;
    }


    public Long getG() {
        return g;
    }


    public void setG(Long g) {
        this.g = g;
    }


    public Integer getH() {
        return h;
    }


    public void setH(Integer h) {
        this.h = h;
    }


    public String getI() {
        return i;
    }


    public void setI(String i) {
        this.i = i;
    }


    public Integer getJ() {
        return j;
    }


    public void setJ(Integer j) {
        this.j = j;
    }


    public boolean isK() {
        return k;
    }


    public void setK(boolean k) {
        this.k = k;
    }
}
