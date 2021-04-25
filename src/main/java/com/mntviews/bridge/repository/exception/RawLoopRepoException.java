package com.mntviews.bridge.repository.exception;

public class RawLoopRepoException extends RuntimeException {
    public RawLoopRepoException() {
    }

    public RawLoopRepoException(String message) {
        super(message);
    }

    public RawLoopRepoException(String message, Throwable cause) {
        super(message, cause);
    }

    public RawLoopRepoException(Throwable cause) {
        super(cause);
    }
}
