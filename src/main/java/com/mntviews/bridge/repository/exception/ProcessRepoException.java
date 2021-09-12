package com.mntviews.bridge.repository.exception;

public class ProcessRepoException extends RuntimeException {
    public ProcessRepoException() {
    }

    public ProcessRepoException(String message) {
        super(message);
    }

    public ProcessRepoException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProcessRepoException(Throwable cause) {
        super(cause);
    }
}
