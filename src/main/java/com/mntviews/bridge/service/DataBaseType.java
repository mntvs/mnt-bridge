package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.impl.MetaInitPostgresqlRepoImpl;
import com.mntviews.bridge.service.impl.DataBaseInitOracleServiceImpl;
import com.mntviews.bridge.service.impl.DataBaseInitPostgresqlServiceImpl;
import com.mntviews.bridge.service.impl.DataBaseInitTestServiceImpl;

import java.sql.Connection;

public enum DataBaseType implements DataBaseInitService {
    POSTGRESQL(new DataBaseInitPostgresqlServiceImpl(new MetaInitPostgresqlRepoImpl())), ORACLE(new DataBaseInitOracleServiceImpl()), TEST(new DataBaseInitTestServiceImpl());

    private final DataBaseInitService dataBaseInitService;

    DataBaseType(DataBaseInitService dataBaseInitService) {
        this.dataBaseInitService = dataBaseInitService;
    }

    @Override
    public void migrate(ConnectionData connectionData) {
        dataBaseInitService.migrate(connectionData);
    }

    @Override
    public void init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName) {
        dataBaseInitService.init(connectionData, groupTag, metaTag, schemaName);
    }

    @Override
    public Connection getConnection(ConnectionData connectionData) {
        return dataBaseInitService.getConnection(connectionData);
    }
}
