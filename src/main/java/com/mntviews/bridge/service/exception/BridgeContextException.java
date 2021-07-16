package com.mntviews.bridge.service.exception;

public class BridgeContextException extends RuntimeException {
    public BridgeContextException() {
    }

    public BridgeContextException(String message) {
        super(message);
    }

    public BridgeContextException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeContextException(Throwable cause) {
        super(cause);
    }
}
