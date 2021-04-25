package com.mntviews.bridge.repository.exception;

public class PreProcessRepoException extends RuntimeException {
    public PreProcessRepoException() {
    }

    public PreProcessRepoException(String message) {
        super(message);
    }

    public PreProcessRepoException(String message, Throwable cause) {
        super(message, cause);
    }

    public PreProcessRepoException(Throwable cause) {
        super(cause);
    }
}
