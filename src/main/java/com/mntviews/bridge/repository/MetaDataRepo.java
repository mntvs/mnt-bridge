package com.mntviews.bridge.repository;

import com.mntviews.bridge.model.MetaData;
import java.sql.Connection;

public interface MetaDataRepo {
    MetaData findMetaData(Connection connection, String groupTag, String metaTag, String schemaName);
}
