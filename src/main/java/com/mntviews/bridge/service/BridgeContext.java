package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.repository.impl.RawLoopRepoImpl;
import com.mntviews.bridge.service.exception.BridgeContextException;
import com.mntviews.bridge.service.impl.BridgeServiceImpl;
import lombok.Getter;

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
        this.connectionData = builder.connectionData;
        this.bridgeProcessing = builder.bridgeProcessing;

        if (builder.dataBaseType == null)
            this.dataBaseType = DEFAULT_DATABASE_TYPE;
        else
            this.dataBaseType = builder.dataBaseType;

        if (builder.bridgeService == null) {
            MetaDataRepo metaDataRepo = new MetaDataRepoImpl();
            RawLoopRepo rawLoopRepo = new RawLoopRepoImpl();
            this.bridgeService = new BridgeServiceImpl(rawLoopRepo, metaDataRepo);
        } else
            this.bridgeService = builder.bridgeService;

        this.schemaName = Objects.requireNonNullElse(builder.schemaName, DEFAULT_SCHEMA_NAME);
    }

    public void execute() {
        if (metaData != null)
            bridgeService.execute(metaData, this.dataBaseType.getConnection(connectionData), bridgeProcessing, connectionData.getSchemaName());
        else throw new BridgeContextException("Bridge context is not initialized");

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

        public Builder withFlyWayService(DataBaseInitService dataBaseInitService) {
            this.dataBaseInitService = dataBaseInitService;
            return this;
        }

        public BridgeContext build() {
            return new BridgeContext(this);
        }
    }
}
