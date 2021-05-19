package com.mntviews.bridge.repository;

import com.mntviews.bridge.model.ConnectionData;

import java.sql.Connection;

public interface MetaInitRepo {
    void init(Connection connection, String groupTag, String metaTag, String schemaName, String schemaMetaName);

    Connection getConnection(ConnectionData connectionData);
}
