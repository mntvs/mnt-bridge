package com.mntviews.bridge.service;

import com.mntviews.bridge.service.exception.BridgeUtilException;

import java.io.*;
import java.util.Properties;

public class BridgeUtil {
    private static final Properties BUILD_INFO;

    public static final int STATUS_INTACT = 0;
    public static final int STATUS_SUCCESS = 1;
    public static final int STATUS_CANCELED = 5;
    public static final int STATUS_ERROR = -3;
    public static final int STATUS_ERROR_UNREPEATABLE = 3;
    public static final String PARAM_ORDER = "ORDER";

    private BridgeUtil() {
    }

    public static Properties getBuildInfo() {
        return BUILD_INFO;
    }

    static {
        try (InputStream inputStream = BridgeUtil.class.getClassLoader().getResourceAsStream("build-info.properties")) {
            BUILD_INFO = new Properties();
            BUILD_INFO.load(inputStream);
        } catch (IOException e) {
            throw new BridgeUtilException(e);
        }
    }
}
