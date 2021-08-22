package com.mntviews.bridge.repository.exception;

public class RawRepoException extends RuntimeException {
    public RawRepoException() {
    }

    public RawRepoException(String message) {
        super(message);
    }

    public RawRepoException(String message, Throwable cause) {
        super(message, cause);
    }

    public RawRepoException(Throwable cause) {
        super(cause);
    }
}
