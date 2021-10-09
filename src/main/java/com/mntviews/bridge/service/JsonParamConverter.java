package com.mntviews.bridge.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mntviews.bridge.service.exception.ParamConverterException;

import java.util.HashMap;
import java.util.Map;

public class JsonParamConverter implements ParamConverter {
    @Override
    public String toString(Map<String, Object> value) {
        try {
            if (value == null)
                return null;
            return new ObjectMapper().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new ParamConverterException(e);
        }
    }

    @Override
    public Map<String, Object> toValue(String param) {
        try {
            if (param == null)
                return new HashMap<>();
            return new ObjectMapper().readValue(param, Map.class);
        } catch (JsonProcessingException e) {
            throw new ParamConverterException(e);
        }
    }
}
