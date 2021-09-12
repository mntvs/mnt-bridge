package com.mntviews.bridge.service;

import java.util.Objects;

public enum DefaultParam {
    ORDER("LIFO"), ATTEMPT(-1);

    private final String valueStr;
    private final Integer valueInt;

    DefaultParam(String value) {
        this.valueStr = value;
        this.valueInt = null;
    }

    DefaultParam(Integer valueInt) {
        this.valueStr = null;
        this.valueInt = valueInt;
    }

    public Object getValue() {
        return Objects.requireNonNullElse(valueStr, valueInt);
    }
}
