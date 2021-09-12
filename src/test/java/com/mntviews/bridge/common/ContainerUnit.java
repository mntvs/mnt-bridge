package com.mntviews.bridge.common;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.service.BridgeContext;
import lombok.Getter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;

public abstract class ContainerUnit {
    public final static String GROUP_TAG = "TEST_GROUP";
    public final static String META_TAG = "TEST_META";
    public final static String SCHEMA_NAME = "TEST_SCHEMA";

    public final static String DB_NAME = "mnt";
    public final static String USER_NAME = BridgeContext.DEFAULT_SCHEMA_NAME;
    public final static String USER_PASSWORD = BridgeContext.DEFAULT_SCHEMA_NAME;
    public final static String TEST_EXCEPTION_TEXT= "Test exception";
    public final static Integer ITEMS_COUNT = 100;

    public final static Integer TASK_COUNT = 20;

    @Getter
    protected JdbcTemplate jdbcTemplate;

    protected Connection connection;

    protected ConnectionData connectionData;

    protected MetaDataRepo metaDataRepo = new MetaDataRepoImpl();
    protected MetaData metaData;


    public String attemptTestParam;

    @Getter
    protected BridgeContext bridgeContext;
    @Getter
    protected BridgeContext bridgeContextAttempt;

    public String wrapCodeBlock(String codeBlock) { return codeBlock; }

    abstract public String findTestProcedure(String typeTag);
    abstract public String findDbTypeName();
}
