package com.mntviews.bridge.service;

import com.mntviews.bridge.model.BufData;
import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.RawData;

import java.sql.Connection;

public interface BridgeService {

    void execute(MetaData metaData, Connection connection, BridgeProcessing bridgeProcessing, String schemaName);

    MetaData findMetaData(String groupTag, String metaTag, Connection connection, String schemaName);

    RawData saveRawData(Connection connection, MetaData metaData, RawData rawData);

    RawData findRawDataById(Connection connection, MetaData metaData, Long id);

    void deleteRawData(Connection connection, MetaData metaData, RawData rawData);

    void deleteRawDataById(Connection connection, MetaData metaData, Long id);

    BufData saveBufDataById(Connection connection, MetaData metaData, BufData bufData);

}
