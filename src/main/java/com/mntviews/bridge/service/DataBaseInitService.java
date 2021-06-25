package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;

import java.sql.Connection;

public interface DataBaseInitService {
    void migrate(ConnectionData connectionData, Boolean isClean);
    void migrate(ConnectionData connectionData);

    void init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName);

    void clear(ConnectionData connectionData, String groupTag, String metaTag);

    Connection getConnection(ConnectionData connectionData);
}
