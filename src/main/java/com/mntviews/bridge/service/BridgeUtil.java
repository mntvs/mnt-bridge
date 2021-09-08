package com.mntviews.bridge.service;

import java.io.*;
import java.util.Properties;

public class BridgeUtil {
    public final static Properties BUILD_INFO;

    public final static int STATUS_INTACT = 0;
    public final static int STATUS_SUCCESS = 1;
    public final static int STATUS_CANCELED = 5;
    public final static int STATUS_ERROR = -3;
    public final static int STATUS_ERROR_UNREPEATABLE = 3;
    public static final String PARAM_ORDER = "ORDER" ;

    public static String findNameServer() {
        String s;
        try {
            Process p = Runtime.getRuntime().exec("cat /etc/resolv.conf");
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((s = stdInput.readLine()) != null) {
                if (s.contains("nameserver"))
                    return s.replace("nameserver", "").trim();

            }
            return "";
        } catch (Exception e) {
            return "";
        }
    }

    static {
        try (InputStream inputStream = BridgeUtil.class.getClassLoader().getResourceAsStream("build-info.properties")) {
            BUILD_INFO = new Properties();
            BUILD_INFO.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
