package com.mntviews.bridge.repository.exception;

public class BufRepoException extends RuntimeException {
    public BufRepoException() {
    }

    public BufRepoException(String message) {
        super(message);
    }

    public BufRepoException(String message, Throwable cause) {
        super(message, cause);
    }

    public BufRepoException(Throwable cause) {
        super(cause);
    }
}
