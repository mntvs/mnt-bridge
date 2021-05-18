package com.mntviews.bridge.service;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.repository.impl.RawLoopRepoImpl;
import com.mntviews.bridge.service.impl.BridgeServiceImpl;
import com.mntviews.bridge.service.impl.FlyWayServicePostgresqlImpl;

import java.util.Objects;

public class BridgeContext {

    public static final String DEFAULT_SCHEMA_NAME = "mnt_bridge";

    private final String groupTag;
    private final String metaTag;
    private final ConnectionData connectionData;
    private final BridgeProcessing bridgeProcessing;
    private final BridgeService bridgeService;
    private final FlyWayService flyWayService;
    private final String schemaName;

    BridgeContext(Builder builder) {
        this.groupTag = builder.groupTag;
        this.metaTag = builder.metaTag;
        this.connectionData = builder.connectionData;
        this.bridgeProcessing = builder.bridgeProcessing;
        this.schemaName = builder.schemaName;
        if (builder.bridgeService == null) {
            MetaDataRepo metaDataRepo = new MetaDataRepoImpl();
            RawLoopRepo rawLoopRepo = new RawLoopRepoImpl();
            this.bridgeService = new BridgeServiceImpl(rawLoopRepo, metaDataRepo);
        } else
            this.bridgeService = builder.bridgeService;

        this.flyWayService = Objects.requireNonNullElseGet(builder.flyWayService, FlyWayServicePostgresqlImpl::new);

    }

    public void execute() {
        bridgeService.execute(groupTag, metaTag, connectionData, bridgeProcessing, schemaName);
    }


    public void migrate() {
        flyWayService.migrate(connectionData);
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
        private FlyWayService flyWayService;


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

        public Builder withFlyWayService(FlyWayService flyWayService) {
            this.flyWayService = flyWayService;
            return this;
        }

        public BridgeContext build() {
            return new BridgeContext(this);
        }
    }
}
