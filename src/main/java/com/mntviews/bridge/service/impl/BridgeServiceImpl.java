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
    public void execute(String groupTag, String metaTag, ConnectionData connectionData, BridgeProcessing bridgeProcessing,String schemaName) {

        Properties props = new Properties();
        props.setProperty("user", connectionData.getUserName());
        props.setProperty("password", connectionData.getPassword());
        props.setProperty("escapeSyntaxCallMode", "callIfNoReturn");
        try (Connection connection = DriverManager.getConnection(connectionData.getUrl(), props)) {
            connection.setAutoCommit(false);
            MetaData metaData = metaDataRepo.findMetaData(connection, groupTag, metaTag, schemaName);
            rawLoopRepo.rawLoop(connection, metaData, bridgeProcessing);
        } catch (SQLException e) {
            throw new BridgeServiceException(e);
        }
    }
}
