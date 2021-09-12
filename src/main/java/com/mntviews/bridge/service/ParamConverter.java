package com.mntviews.bridge.service;

import java.util.Map;

public interface ParamConverter {
    String toString(Map<String,Object> value);
    Map<String,Object> toValue(String param);
}
