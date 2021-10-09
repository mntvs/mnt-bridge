package com.mntviews.bridge.repository.exception;

public class UnrepeatableStatusException extends RuntimeException {

    static final String CODE = "20993";

    public UnrepeatableStatusException() {
    }

    public UnrepeatableStatusException(String message) {
        super(message);
    }

    public UnrepeatableStatusException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnrepeatableStatusException(Throwable cause) {
        super(cause);
    }
}
