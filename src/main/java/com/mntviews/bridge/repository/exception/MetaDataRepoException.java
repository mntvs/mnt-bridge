package com.mntviews.bridge.repository.exception;

public class MetaDataRepoException extends RuntimeException {
    public MetaDataRepoException() {
    }

    public MetaDataRepoException(String message) {
        super(message);
    }

    public MetaDataRepoException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetaDataRepoException(Throwable cause) {
        super(cause);
    }
}
