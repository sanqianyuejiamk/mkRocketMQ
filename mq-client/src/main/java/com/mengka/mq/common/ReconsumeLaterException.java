package com.mengka.mq.common;

/**
 * User: xiafeng
 * Date: 15-8-6-22:18
 */
public class ReconsumeLaterException extends Exception {

    private static final long serialVersionUID = 1L;

    private final int responseCode;
    private final String errorMessage;

    public ReconsumeLaterException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public ReconsumeLaterException(String errorMessage) {
        super("DESC: " + errorMessage);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
