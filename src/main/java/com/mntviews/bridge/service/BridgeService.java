package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;

import java.sql.Connection;

public interface BridgeService {

    void execute(MetaData metaData, Connection connection, BridgeProcessing bridgeProcessing, String schemaName);

    MetaData findMetaData(String groupTag, String metaTag, Connection connection, String schemaName);
}
