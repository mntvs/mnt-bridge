package com.mntviews.bridge.service;

import com.mntviews.bridge.common.PostgresContainerUnitOld;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.repository.impl.RawLoopRepoImpl;
import com.mntviews.bridge.service.impl.BridgeServiceImpl;
import org.junit.jupiter.api.Test;

public class BridgeServiceTest extends PostgresContainerUnitOld {

    @Test
    public void executeTest() {
        MetaDataRepo metaDataRepo = new MetaDataRepoImpl();
        RawLoopRepo rawLoopRepo = new RawLoopRepoImpl();
        BridgeService bridgeService = new BridgeServiceImpl(rawLoopRepo, metaDataRepo);
        bridgeService.execute(GROUP_TAG, META_TAG,connection, null, BridgeContext.DEFAULT_SCHEMA_NAME);
    }
}
