package com.mntviews.bridge.common;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.DataBaseType;


public class PostgresContainerUnit extends ContainerUnit {


    public final static String DB_URL = "jdbc:postgresql://localhost:5432/postgres";

    public final static String USER_NAME = "postgres";
    public final static String USER_PASSWORD = "123";

    public PostgresContainerUnit() {
        connectionData = new ConnectionData(DB_URL, USER_NAME, USER_PASSWORD, BridgeContext.DEFAULT_SCHEMA_NAME);

        bridgeContext = BridgeContext
                .custom(GROUP_TAG, META_TAG, connectionData)
                .withBridgeProcessing((connection, processData) -> {
                    if (processData.getRawId() < 0)
                        throw new RuntimeException(ERROR_LOG_TEXT);
                })
                .withSchemaName(SCHEMA_NAME)
                .withDataBaseType(DataBaseType.POSTGRESQL)
                .build();
    }
}
