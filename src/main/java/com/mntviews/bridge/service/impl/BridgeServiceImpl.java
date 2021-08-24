package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.BufData;
import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.RawData;
import com.mntviews.bridge.repository.BufRepo;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.RawRepo;
import com.mntviews.bridge.service.BridgeProcessing;
import com.mntviews.bridge.service.BridgeService;
import com.mntviews.bridge.service.exception.BridgeServiceException;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@RequiredArgsConstructor
public class BridgeServiceImpl implements BridgeService {

    private final RawLoopRepo rawLoopRepo;
    private final MetaDataRepo metaDataRepo;
    private final RawRepo rawRepo;
    private final BufRepo bufRepo;

    @Override
    public void execute(MetaData metaData, Connection connection, BridgeProcessing bridgeProcessing, String schemaName) {

            rawLoopRepo.rawLoop(connection, metaData, bridgeProcessing, schemaName);
    }

    @Override
    public MetaData findMetaData(String groupTag, String metaTag, Connection connection, String schemaName) {
        return metaDataRepo.findMetaData(connection, groupTag, metaTag, schemaName);
    }

    @Override
    public RawData saveRawData(Connection connection, MetaData metaData, RawData rawData) {
        return rawRepo.saveRawData(connection,metaData.getRawFullName(),rawData);
    }

    @Override
    public RawData findRawDataById(Connection connection, MetaData metaData, Long id) {
        return rawRepo.findRawDataById(connection, metaData.getRawFullName(), id);
    }

    @Override
    public BufData findBufDataById(Connection connection, MetaData metaData, Long id) {
        return bufRepo.findBufDataById(connection, metaData.getBufFullName(), id);
    }

    @Override
    public BufData findBufDataByRawId(Connection connection, MetaData metaData, Long id) {
        return bufRepo.findBufDataByRawId(connection, metaData.getBufFullName(), id);
    }

    @Override
    public void deleteRawData(Connection connection, MetaData metaData, RawData rawData) {

    }

    @Override
    public void deleteRawDataById(Connection connection, MetaData metaData, Long id) {

    }

    @Override
    public BufData saveBufDataById(Connection connection, MetaData metaData, BufData bufData) {
        return null;
    }


}
