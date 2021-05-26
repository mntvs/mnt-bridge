package com.mntviews.bridge.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BridgeUtilTest {
    @Test
    void findNameServerTest() {
        String nameServer = BridgeUtil.findNameServer();
        System.out.println(nameServer);
        assertNotNull(nameServer);
    }
}
