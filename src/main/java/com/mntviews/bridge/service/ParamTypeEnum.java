package com.mntviews.bridge.service;

import java.util.Map;

public enum ParamTypeEnum implements ParamConverter {
    JSON(new JsonParamConverter()), XML(new XmlParamConverter());

    private final ParamConverter paramConverter;

    ParamTypeEnum(ParamConverter paramConverter) {
        this.paramConverter = paramConverter;
    }

    @Override
    public String toString(Map<String, Object> value) {
        return paramConverter.toString(value);
    }

    @Override
    public Map<String, Object> toValue(String param) {
        return paramConverter.toValue(param);
    }
}
