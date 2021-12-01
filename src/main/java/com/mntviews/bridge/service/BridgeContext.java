package com.mntviews.bridge.service;

import com.mntviews.bridge.model.BufData;
import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.RawData;
import com.mntviews.bridge.repository.BufRepo;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.RawRepo;
import com.mntviews.bridge.repository.impl.BufRepoImpl;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.repository.impl.RawLoopRepoImpl;
import com.mntviews.bridge.repository.impl.RawRepoImpl;
import com.mntviews.bridge.service.exception.BridgeContextException;
import com.mntviews.bridge.service.impl.BridgeServiceImpl;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.mntviews.bridge.service.BridgeUtil.nvl;

public class BridgeContext implements Bridge {

    public static final String DEFAULT_SCHEMA_NAME = "mnt_bridge";
    public static final DataBaseType DEFAULT_DATABASE_TYPE = DataBaseType.POSTGRESQL;
    private final String groupTag;
    private final String metaTag;
    private final ConnectionData connectionData;
    private final BridgeProcessing bridgeBeforeProcessing;
    private final BridgeProcessing bridgeAfterProcessing;
    private final BridgeService bridgeService;
    private final String schemaName;
    private final DataBaseType dataBaseType;
    private final Map<String, Object> param;

    @Getter
    private MetaData metaData;

    BridgeContext(Builder builder) {
        this.groupTag = builder.groupTag;
        this.metaTag = builder.metaTag;

        this.bridgeBeforeProcessing = builder.beforeProcessing;
        this.bridgeAfterProcessing = builder.afterProcessing;

        if (builder.dataBaseType == null)
            this.dataBaseType = DEFAULT_DATABASE_TYPE;
        else
            this.dataBaseType = builder.dataBaseType;

        if (builder.bridgeService == null) {
            MetaDataRepo metaDataRepo = new MetaDataRepoImpl();
            RawLoopRepo rawLoopRepo = new RawLoopRepoImpl();
            RawRepo rawRepo = new RawRepoImpl();
            BufRepo bufRepo = new BufRepoImpl();
            this.bridgeService = new BridgeServiceImpl(rawLoopRepo, metaDataRepo, rawRepo, bufRepo);
        } else
            this.bridgeService = builder.bridgeService;

        this.schemaName = nvl(builder.schemaName, DEFAULT_SCHEMA_NAME);

        this.connectionData = new ConnectionData(builder.connectionData.getUrl(), builder.connectionData.getUserName()
                , builder.connectionData.getPassword(), nvl(builder.connectionData.getSchemaName(), this.schemaName));

        if (builder.param != null) {
            // complete param list with default params
            Map<String, Object> fullParam = Arrays.stream(DefaultParam.values()).collect(Collectors.toMap(DefaultParam::toString, DefaultParam::getValue));
            fullParam.putAll(builder.param);
            this.param = fullParam;
        } else
            this.param = null;
    }

    private void checkMetaData() {
        if (metaData == null)
            throw new BridgeContextException("Bridge context is not initialized");
    }

    @Override
    public void execute(Connection connection) {
        bridgeService.execute(metaData, connection, bridgeBeforeProcessing, bridgeAfterProcessing, connectionData.getSchemaName(), param);
    }

    @Override
    public void execute() {
        checkMetaData();
        Connection connection = findConnection();
        execute(connection);
        closeConnection(connection);
    }

    @Override
    public void executeOne(Long rawId, Connection connection) {
        bridgeService.executeOne(metaData, connection, bridgeBeforeProcessing, bridgeAfterProcessing, connectionData.getSchemaName(), rawId, param);
    }

    @Override
    public void executeOne(Long rawId) {
        checkMetaData();
        Connection connection = findConnection();
        executeOne(rawId, connection);
        closeConnection(connection);
    }

    @Override
    public void executeGroup(String groupId, Connection connection) {
        bridgeService.executeGroup(metaData, connection, bridgeBeforeProcessing, bridgeAfterProcessing, connectionData.getSchemaName(), groupId, param);
    }

    @Override
    public void executeGroup(String groupId) {
        checkMetaData();
        Connection connection = findConnection();
        executeGroup(groupId, connection);
        closeConnection(connection);
    }

    @Override
    public ConnectionData getConnectionData() {
        return connectionData;
    }

    @Override
    public RawData findRawDataById(Long id, Connection connection) {
        checkMetaData();
        return bridgeService.findRawDataById(connection, metaData, id);

    }

    @Override
    public void closeConnection(Connection connection) {
        try {
            if (connection != null) connection.close();
        } catch (SQLException e) {
            throw new BridgeContextException(e);
        }
    }

    @Override
    public RawData findRawDataById(Long id) {
        checkMetaData();
        Connection connection = findConnection();
        RawData rawData = bridgeService.findRawDataById(findConnection(), metaData, id);
        closeConnection(connection);
        return rawData;
    }

    @Override
    public RawData saveRawData(RawData rawData, Connection connection) {
        checkMetaData();
        return bridgeService.saveRawData(connection, metaData, rawData);
    }

    @Override
    public RawData saveRawData(RawData rawData) {
        Connection connection = findConnection();
        RawData rawDataNew = saveRawData(rawData, connection);
        closeConnection(connection);
        return rawDataNew;
    }

    @Override
    public BufData saveBufData(BufData bufData, Connection connection) {
        checkMetaData();
        return bridgeService.saveBufData(connection, metaData, bufData);
    }

    @Override
    public BufData saveBufData(BufData bufData) {
        checkMetaData();
        return bridgeService.saveBufData(findConnection(), metaData, bufData);
    }

    @Override
    public BufData findBufDataById(Long id, Connection connection) {
        checkMetaData();
        return bridgeService.findBufDataById(connection, metaData, id);
    }

    @Override
    public BufData findBufDataByRawId(Long id, Connection connection) {
        checkMetaData();
        return bridgeService.findBufDataByRawId(connection, metaData, id);
    }


    @Override
    public void migrate(Boolean isClean) {
        dataBaseType.migrate(connectionData, isClean);
    }

    @Override
    public void migrate() {
        dataBaseType.migrate(connectionData, false);
    }

    @Override
    public void init() {
        metaData = dataBaseType.init(connectionData, groupTag, metaTag, schemaName, param);
    }

    @Override
    public void clear() {
        dataBaseType.clear(connectionData, groupTag, metaTag);
    }

    @Override
    public Connection findConnection() {
        return dataBaseType.getConnection(connectionData);
    }

    @Override
    public String getGroupTag() {
        return groupTag;
    }

    @Override
    public String getMetaTag() {
        return metaTag;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }


    public static Builder custom(String groupTag, String metaTag, ConnectionData connectionData) {
        return new Builder(groupTag, metaTag, connectionData);
    }

    public static class Builder {
        private final String groupTag;
        private final String metaTag;
        private final ConnectionData connectionData;
        private BridgeProcessing beforeProcessing;
        private BridgeProcessing afterProcessing;

        private String schemaName;
        private BridgeService bridgeService;
        private DataBaseType dataBaseType;
        private Map<String, Object> param;

        public Builder(String groupTag, String metaTag, ConnectionData connectionData) {
            this.groupTag = groupTag;
            this.metaTag = metaTag;
            this.connectionData = connectionData;
        }

        public Builder withBeforeProcessing(BridgeProcessing beforeProcessing) {
            this.beforeProcessing = beforeProcessing;
            return this;
        }

        public Builder withAfterProcessing(BridgeProcessing afterProcessing) {
            this.afterProcessing = afterProcessing;
            return this;
        }

        public Builder withBridgeService(BridgeService bridgeService) {
            this.bridgeService = bridgeService;
            return this;
        }


        public Builder withSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }


        public Builder withDataBaseType(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
            return this;
        }


        public Builder withParam(Map<String, Object> param) {
            this.param = param;
            return this;
        }

        public BridgeContext build() {
            return new BridgeContext(this);
        }
    }
}
