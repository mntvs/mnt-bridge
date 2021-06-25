package com.mntviews.bridge.common;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.DataBaseType;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;

//@Testcontainers
public class OracleContainerUnit extends ContainerUnit {

    public final static String DB_URL = "jdbc:oracle:thin:@localhost:1521:xe";

    public final static String USER_NAME = "mnt_bridge";
    public final static String USER_PASSWORD = "mnt_bridge";


//    @Container
//    protected OracleContainer  oracleContainer = new OracleContainer("oracle/database_prebuild:18.4.0-xe").withUsername("system").withPassword("master").withReuse(true);

    public OracleContainerUnit() {
        connectionData = new ConnectionData(DB_URL, USER_NAME, USER_PASSWORD, BridgeContext.DEFAULT_SCHEMA_NAME);

        bridgeContext = BridgeContext
                .custom(GROUP_TAG, META_TAG, connectionData)
                .withBridgeProcessing((connection, processData) -> {
                })
                .withSchemaName(SCHEMA_NAME)
                .withDataBaseType(DataBaseType.ORACLE)
                .build();


    }

}