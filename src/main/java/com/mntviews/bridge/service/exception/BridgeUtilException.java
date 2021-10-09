package com.mntviews.bridge.service.exception;

public class BridgeUtilException extends RuntimeException {
    public BridgeUtilException() {
    }

    public BridgeUtilException(String message) {
        super(message);
    }

    public BridgeUtilException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeUtilException(Throwable cause) {
        super(cause);
    }
}
