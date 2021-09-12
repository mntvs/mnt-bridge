package com.mntviews.bridge.service.exception;

public class ParamConverterException extends RuntimeException {
    public ParamConverterException() {
    }

    public ParamConverterException(String message) {
        super(message);
    }

    public ParamConverterException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParamConverterException(Throwable cause) {
        super(cause);
    }
}
