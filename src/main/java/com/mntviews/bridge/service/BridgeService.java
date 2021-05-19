package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;

import java.sql.Connection;

public interface BridgeService {

    void execute(String groupTag, String metaTag, Connection connection, BridgeProcessing bridgeProcessing, String schemaName);
}
