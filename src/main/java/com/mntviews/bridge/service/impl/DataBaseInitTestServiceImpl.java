package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.DataBaseInitService;

import java.sql.Connection;

public class DataBaseInitTestServiceImpl implements DataBaseInitService {
    @Override
    public void migrate(ConnectionData connectionData, Boolean isDropSchema) {

    }

    @Override
    public void migrate(ConnectionData connectionData) {

    }

    @Override
    public void init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName) {

    }

    @Override
    public Connection getConnection(ConnectionData connectionData) {
        return null;
    }
}
