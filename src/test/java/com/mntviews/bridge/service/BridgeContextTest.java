package com.mntviews.bridge.service;

import com.mntviews.bridge.common.PostgresContainerTest;
import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.impl.MetaInitPostgresqlRepoImpl;
import com.mntviews.bridge.service.impl.DataBaseInitPostgresqlServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
public class BridgeContextTest {

    @Mock
    BridgeService bridgeService;


    @BeforeEach
    void init() {
        doNothing().when(bridgeService).execute(isA(String.class), isA(String.class),isNull(), isA(BridgeProcessing.class), isA(String.class));
    }

    @Test
    public void executeBridgeContextTest() {


        BridgeContext bridgeContext = BridgeContext
                .custom("GROUP_TAG", "META_TAG", new ConnectionData("URL", "USER_NAME", "PASSWORD", "DEFAULT_SCHEMA"))
                .withBridgeProcessing((connection, processData) -> {
                })
                .withBridgeService(bridgeService)
                .withDataBaseType(DataBaseType.TEST)
                .build();
        bridgeContext.execute();
    }


}
