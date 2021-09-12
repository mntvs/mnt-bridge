package com.mntviews.bridge.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.mntviews.bridge.service.exception.ParamConverterException;

import java.util.Map;

public class XmlParamConverter implements ParamConverter {
    @Override
    public String toString(Map<String, Object> value) {
        try {
            if (value == null)
                return null;
            return new XmlMapper().writer().withRootName("PARAM").writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new ParamConverterException(e);
        }
    }

    @Override
    public Map<String, Object> toValue(String param) {
        try {
            if (param == null)
                return null;
            return new XmlMapper().readValue(param, Map.class);
        } catch (JsonProcessingException e) {
            throw new ParamConverterException(e);
        }
    }
}
