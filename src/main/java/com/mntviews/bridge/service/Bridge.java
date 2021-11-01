package com.mntviews.bridge.service;

import com.mntviews.bridge.model.BufData;
import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.RawData;

import java.sql.Connection;

public interface Bridge {
    void init();
    void clear();

    ConnectionData getConnectionData();

    RawData findRawDataById(Long id, Connection connection);

    void closeConnection(Connection connection);

    RawData findRawDataById(Long id);

    RawData saveRawData(RawData rawData, Connection connection);
    RawData saveRawData(RawData rawData);

    BufData saveBufData(BufData bufData, Connection connection);

    BufData saveBufData(BufData bufData);

    BufData findBufDataById(Long id, Connection connection);

    BufData findBufDataByRawId(Long id, Connection connection);

    void migrate(Boolean isClean);
    void migrate();

    String getGroupTag();

    String getMetaTag();

    String getSchemaName();

    void execute(Connection connection);

    void execute();

    void executeOne(Long rawId, Connection connection);

    void executeOne(Long rawId);

    void executeGroup(String groupId, Connection connection);

    void executeGroup(String groupId);

    Connection findConnection();
}
