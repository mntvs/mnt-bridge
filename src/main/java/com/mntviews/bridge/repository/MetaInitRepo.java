package com.mntviews.bridge.repository;

import java.sql.Connection;

public interface MetaInitRepo {
    void init(Connection connection, String groupTag, String metaTag, String schemaName, String schemaMetaName);
}
