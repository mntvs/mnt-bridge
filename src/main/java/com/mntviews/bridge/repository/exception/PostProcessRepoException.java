package com.mntviews.bridge.repository.exception;


public class PostProcessRepoException extends RuntimeException {
    public PostProcessRepoException() {
    }

    public PostProcessRepoException(String message) {
        super(message);
    }

    public PostProcessRepoException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostProcessRepoException(Throwable cause) {
        super(cause);
    }
}

