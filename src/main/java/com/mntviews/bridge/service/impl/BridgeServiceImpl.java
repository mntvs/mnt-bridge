package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.service.BridgeProcessing;
import com.mntviews.bridge.service.BridgeService;
import com.mntviews.bridge.service.exception.BridgeServiceException;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@RequiredArgsConstructor
public class BridgeServiceImpl implements BridgeService {

    private final RawLoopRepo rawLoopRepo;
    private final MetaDataRepo metaDataRepo;

    @Override
    public void execute(MetaData metaData, Connection connection, BridgeProcessing bridgeProcessing, String schemaName) {

            rawLoopRepo.rawLoop(connection, metaData, bridgeProcessing, schemaName);
    }

    @Override
    public MetaData findMetaData(String groupTag, String metaTag, Connection connection, String schemaName) {
        return metaDataRepo.findMetaData(connection, groupTag, metaTag, schemaName);
    }

}
