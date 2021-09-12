package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;

import java.sql.Connection;
import java.util.Map;

public interface DataBaseInitService {
    void migrate(ConnectionData connectionData, Boolean isClean);
    void migrate(ConnectionData connectionData);

    MetaData init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName, Map<String, Object> param);

    void clear(ConnectionData connectionData, String groupTag, String metaTag);

    Connection getConnection(ConnectionData connectionData);
}
