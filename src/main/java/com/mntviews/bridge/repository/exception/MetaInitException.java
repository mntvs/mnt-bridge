package com.mntviews.bridge.repository.exception;

public class MetaInitException extends RuntimeException {
    public MetaInitException() {
    }

    public MetaInitException(String message) {
        super(message);
    }

    public MetaInitException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetaInitException(Throwable cause) {
        super(cause);
    }
}
