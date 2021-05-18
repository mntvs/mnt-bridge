package com.mntviews.bridge.service;

import com.mntviews.bridge.common.PostgresContainerTest;
import com.mntviews.bridge.model.ConnectionData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
public class BridgeContextTest {

    @Mock
    BridgeService bridgeService;

    @BeforeEach
    void init() {
        doNothing().when(bridgeService).execute(isA(String.class), isA(String.class), isA(ConnectionData.class), isA(BridgeProcessing.class), isA(String.class));
    }

    @Test
    public void executeBridgeContextTest() {
        BridgeContext bridgeContext = BridgeContext
                .custom("GROUP_TAG", "META_TAG", new ConnectionData("URL", "USE_NAME", "PASSWORD"))
                .withBridgeProcessing((connection, processData) -> {
                })
                .withBridgeService(bridgeService)
                .build();
        bridgeContext.execute();
    }


}
