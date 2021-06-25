package com.mntviews.bridge.service.exception;

public class DataBaseInitServiceException extends RuntimeException {
    public DataBaseInitServiceException() {
    }

    public DataBaseInitServiceException(String message) {
        super(message);
    }

    public DataBaseInitServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataBaseInitServiceException(Throwable cause) {
        super(cause);
    }
}
