package com.mntviews.bridge.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class BridgeUtil {
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
}
