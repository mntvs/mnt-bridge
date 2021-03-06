package com.mntviews.bridge.repository;

import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.ProcessData;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.BridgeProcessing;

import java.sql.Connection;
import java.util.Map;

public interface RawLoopRepo {

    void rawLoop(BridgeContext bridgeContext, MetaData metaData, BridgeProcessing beforeProcessing, BridgeProcessing afterProcessing, String schemaName, Long rawId, String groupId, Map<String, Object> param);

    void preProcess(Connection connection, ProcessData processData, String schemaName);

    void postProcess(Connection connection, ProcessData processData, String schemaName);

    void process(Connection connection, ProcessData processData, String schemaName);

}
