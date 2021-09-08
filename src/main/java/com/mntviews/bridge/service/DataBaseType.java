package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.impl.MetaInitOracleRepoImpl;
import com.mntviews.bridge.repository.impl.MetaInitPostgresqlRepoImpl;
import com.mntviews.bridge.service.impl.DataBaseInitOracleServiceImpl;
import com.mntviews.bridge.service.impl.DataBaseInitPostgresqlServiceImpl;
import com.mntviews.bridge.service.impl.DataBaseInitTestServiceImpl;

import java.sql.Connection;

public enum DataBaseType implements DataBaseInitService {
    POSTGRESQL(new DataBaseInitPostgresqlServiceImpl(new MetaInitPostgresqlRepoImpl()))
    , ORACLE(new DataBaseInitOracleServiceImpl(new MetaInitOracleRepoImpl()))
    , TEST(new DataBaseInitTestServiceImpl(null));

    private final DataBaseInitService dataBaseInitService;

    DataBaseType(DataBaseInitService dataBaseInitService) {
        this.dataBaseInitService = dataBaseInitService;
    }

    @Override
    public void migrate(ConnectionData connectionData, Boolean isClean) {
        dataBaseInitService.migrate(connectionData, isClean);
    }

    @Override
    public void migrate(ConnectionData connectionData) {
        migrate(connectionData, false);
    }

    @Override
    public MetaData init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName, String param) {
        return dataBaseInitService.init(connectionData, groupTag, metaTag, schemaName, param);
    }

    @Override
    public void clear(ConnectionData connectionData, String groupTag, String metaTag) {
        dataBaseInitService.clear(connectionData, groupTag, metaTag);
    }

    @Override
    public Connection getConnection(ConnectionData connectionData) {
        return dataBaseInitService.getConnection(connectionData);
    }
}
