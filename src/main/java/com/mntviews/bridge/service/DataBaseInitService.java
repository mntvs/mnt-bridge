package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;

import java.sql.Connection;

public interface DataBaseInitService {
    void migrate(ConnectionData connectionData, Boolean isClean);
    void migrate(ConnectionData connectionData);

    MetaData init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName);

    void clear(ConnectionData connectionData, String groupTag, String metaTag);

    Connection getConnection(ConnectionData connectionData);
}
