package com.peng.framework.frameworksdkutil.exception;

/**
 * Json 操作异常
 */
public class JsonOperationException extends RuntimeException {
    private String message;

    public JsonOperationException(String message) {
        this.message = message;
    }

    public JsonOperationException() {
    }

    public String getMessage() {
        return this.message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
