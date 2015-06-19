package com.tongbanjie.rocketmq.monitor.common;

/**
 * User: mengka
 * Date: 15-6-19-ÏÂÎç10:52
 */
public class MQException extends Exception {

    private static final long serialVersionUID = 1L;

    private final int errorCode;

    private final String errorMessage;

    public MQException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
        this.errorCode = -1;
        this.errorMessage = errorMessage;
    }

    public MQException(int errorCode, String errorMessage) {
        super("CODE: " + errorCode + "  DESC: " + errorMessage);
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public MQException(String errorMessage) {
        super(errorMessage);
        this.errorCode = -1;
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
