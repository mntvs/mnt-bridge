package com.mntviews.bridge.repository;

import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.ProcessData;
import com.mntviews.bridge.service.BridgeProcessing;

import java.sql.Connection;

public interface RawLoopRepo {

    void rawLoop(Connection connection, MetaData metaData, BridgeProcessing bridgeProcessing, String schemaName, Long rawId);

    void preProcess(Connection connection,ProcessData processData, String schemaName);

    void postProcess(Connection connection,ProcessData processData, String schemaName);
}
