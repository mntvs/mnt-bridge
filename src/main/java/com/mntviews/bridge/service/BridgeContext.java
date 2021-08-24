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

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Objects;

public class BridgeContext {

    public static final String DEFAULT_SCHEMA_NAME = "mnt_bridge";
    public static final DataBaseType DEFAULT_DATABASE_TYPE = DataBaseType.POSTGRESQL;

    private final String groupTag;
    private final String metaTag;
    private final ConnectionData connectionData;
    private final BridgeProcessing bridgeProcessing;
    private final BridgeService bridgeService;
    private final String schemaName;
    private final DataBaseType dataBaseType;

    @Getter
    private MetaData metaData;

    BridgeContext(Builder builder) {
        this.groupTag = builder.groupTag;
        this.metaTag = builder.metaTag;

        this.bridgeProcessing = builder.bridgeProcessing;

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

        this.schemaName = Objects.requireNonNullElse(builder.schemaName, DEFAULT_SCHEMA_NAME);

        this.connectionData = new ConnectionData(builder.connectionData.getUrl(), builder.connectionData.getUserName()
                , builder.connectionData.getPassword(), Objects.requireNonNullElse(builder.connectionData.getSchemaName()
                , this.schemaName));
    }

    private void checkMetaData() {
        if (metaData == null)
            throw new BridgeContextException("Bridge context is not initialized");
    }

    public void execute() {
        checkMetaData();
        bridgeService.execute(metaData, this.dataBaseType.getConnection(connectionData), bridgeProcessing, connectionData.getSchemaName());
    }

    public void saveRawData(RawData rawData) {
        checkMetaData();
        bridgeService.saveRawData(this.dataBaseType.getConnection(connectionData), metaData, rawData);
    }

    public RawData findRawDataById(Long id) {
        return findRawDataById(id, this.dataBaseType.getConnection(connectionData));
    }

    public RawData findRawDataById(Long id, Connection connection) {
        checkMetaData();
        return bridgeService.findRawDataById(connection, metaData, id);

    }

    public BufData findBufDataById(Long id) {
        checkMetaData();
        return findBufDataById(id, this.dataBaseType.getConnection(connectionData));
    }

    public BufData findBufDataById(Long id, Connection connection) {
        checkMetaData();
        return bridgeService.findBufDataById(connection, metaData, id);
    }

    public BufData findBufDataByRawId(Long id) {
        checkMetaData();
        return findBufDataByRawId(id, this.dataBaseType.getConnection(connectionData));
    }


    public BufData findBufDataByRawId(Long id, Connection connection) {
        checkMetaData();
        return bridgeService.findBufDataByRawId(connection, metaData, id);
    }


    public void migrate(Boolean isClean) {
        dataBaseType.migrate(connectionData, isClean);
    }

    public void migrate() {
        dataBaseType.migrate(connectionData, false);
    }


    public void init() {
        metaData = dataBaseType.init(connectionData, groupTag, metaTag, schemaName);
    }

    public void clear() {
        dataBaseType.clear(connectionData, groupTag, metaTag);
    }

    public Connection getConnection() {
        return dataBaseType.getConnection(connectionData);
    }

    public static Builder custom(String groupTag, String metaTag, ConnectionData connectionData) {
        return new Builder(groupTag, metaTag, connectionData);
    }

    public static class Builder {
        private final String groupTag;
        private final String metaTag;
        private final ConnectionData connectionData;
        private BridgeProcessing bridgeProcessing;
        private String schemaName;
        private BridgeService bridgeService;
        private DataBaseInitService dataBaseInitService;
        private DataBaseType dataBaseType;

        public Builder(String groupTag, String metaTag, ConnectionData connectionData) {
            this.groupTag = groupTag;
            this.metaTag = metaTag;
            this.connectionData = connectionData;
        }

        public Builder withBridgeProcessing(BridgeProcessing bridgeProcessing) {
            this.bridgeProcessing = bridgeProcessing;
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

        public BridgeContext build() {
            return new BridgeContext(this);
        }
    }
}
