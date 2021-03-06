package com.mntviews.bridge.service;

import com.mntviews.bridge.model.BufData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.RawData;

import java.sql.Connection;
import java.util.Map;

public interface BridgeService {

    void execute(MetaData metaData, BridgeContext bridgeContext, BridgeProcessing beforeProcessing, BridgeProcessing afterProcessing, String schemaName, Map<String, Object> param);

    void executeOne(MetaData metaData, BridgeContext bridgeContext, BridgeProcessing beforeProcessing, BridgeProcessing afterProcessing, String schemaName, Long rawId, Map<String, Object> param);

    void executeGroup(MetaData metaData, BridgeContext bridgeContext, BridgeProcessing beforeProcessing, BridgeProcessing afterProcessing, String schemaName, String groupId, Map<String, Object> param);

    MetaData findMetaData(String groupTag, String metaTag, Connection connection, String schemaName);

    RawData saveRawData(Connection connection, MetaData metaData, RawData rawData);

    RawData findRawDataById(Connection connection, MetaData metaData, Long id);

    BufData findBufDataById(Connection connection, MetaData metaData, Long id);


    BufData findBufDataByRawId(Connection connection, MetaData metaData, Long id);

    void deleteRawData(Connection connection, MetaData metaData, RawData rawData);

    void deleteRawDataById(Connection connection, MetaData metaData, Long id);

    BufData saveBufData(Connection connection, MetaData metaData, BufData bufData);

}
