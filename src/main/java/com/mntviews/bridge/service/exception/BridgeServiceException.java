package com.mntviews.bridge.service.exception;

public class BridgeServiceException extends  RuntimeException {
    public BridgeServiceException() {
    }

    public BridgeServiceException(String message) {
        super(message);
    }

    public BridgeServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeServiceException(Throwable cause) {
        super(cause);
    }
}
