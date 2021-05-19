package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.repository.impl.RawLoopRepoImpl;
import com.mntviews.bridge.service.impl.BridgeServiceImpl;
import com.mntviews.bridge.service.impl.DataBaseInitPostgresqlServiceImpl;

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

    BridgeContext(Builder builder) {
        this.groupTag = builder.groupTag;
        this.metaTag = builder.metaTag;
        this.connectionData = builder.connectionData;
        this.bridgeProcessing = builder.bridgeProcessing;
        this.schemaName = builder.schemaName;

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

    }

    public void execute() {
        bridgeService.execute(groupTag, metaTag, this.dataBaseType.getConnection(connectionData), bridgeProcessing, schemaName);
    }


    public void init() {
        dataBaseType.migrate(connectionData);
        dataBaseType.init(connectionData,groupTag,metaTag,schemaName);
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
            this.schemaName = DEFAULT_SCHEMA_NAME;
        }

        public Builder withBridgeProcessing(BridgeProcessing bridgeProcessing) {
            this.bridgeProcessing = bridgeProcessing;
            return this;
        }


        public Builder withBridgeService(BridgeService bridgeService) {
            this.bridgeService = bridgeService;
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
