package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.ProcessData;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.exception.PostProcessRepoException;
import com.mntviews.bridge.repository.exception.PreProcessRepoException;
import com.mntviews.bridge.repository.exception.RawLoopRepoException;
import com.mntviews.bridge.repository.exception.UnrepeatableStatusException;
import com.mntviews.bridge.service.BridgeProcessing;
import com.mntviews.bridge.service.BridgeUtil;
import lombok.RequiredArgsConstructor;

import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor
public class RawLoopRepoImpl implements RawLoopRepo {

    /**
     * Main loop to process raw queue
     * TODO: Modify to start with only provided raw_id
     * TODO: Option to change order
     *  @param connection       opened connection with db
     *
     * @param metaData         system data received from db
     * @param bridgeProcessing outer procedure to process current raw
     * @param schemaName       schema name for system system objects
     * @param rawId
     */
    @Override
    public void rawLoop(Connection connection, MetaData metaData, BridgeProcessing bridgeProcessing, String schemaName, Long rawId) {
        AtomicInteger count = new AtomicInteger();

        String rawLoopQuery;
        if (rawId == null) {
            Object paramOrder = metaData.getParam().get(BridgeUtil.PARAM_ORDER);
            if (paramOrder == null)
                throw new RawLoopRepoException("parameter '" + BridgeUtil.PARAM_ORDER + "' is not defined");

            if (!(paramOrder instanceof String))
                throw new RawLoopRepoException("parameter '" + BridgeUtil.PARAM_ORDER + "' is must be String");

            switch ((String) paramOrder) {
                case "LIFO":
                    rawLoopQuery = "select id from " + metaData.getRawFullName() + " where s_action=0 order by s_date desc, id desc";
                    break;
                case "FIFO":
                    rawLoopQuery = "select id from " + metaData.getRawFullName() + " where s_action=0 order by s_date asc, id asc";
                    break;
                default:
                    throw new RawLoopRepoException("Parameter '" + BridgeUtil.PARAM_ORDER + "' must be FIFO or LIFO");
            }

        }
        else
            rawLoopQuery = "select id from " + metaData.getRawFullName() + " where s_action=0 and id=?";
        try (PreparedStatement stmt = connection.prepareStatement(rawLoopQuery)) {
            if (rawId != null)
                stmt.setLong(1, rawId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Integer processedStatus = 0;
                    String errorMessage = null;
                    ProcessData processData = new ProcessData();
                    processData.setRawId(rs.getLong("id"));
                    processData.setMetaData(metaData);
                    processData.setProcessedStatus(processedStatus);
                    processData.setErrorMessage(errorMessage);
                    preProcess(connection, processData, schemaName);
                    if (processData.getProcessedStatus() == 1) {
                        try {
                            if (bridgeProcessing != null)
                                bridgeProcessing.process(connection, processData);
                        } catch (UnrepeatableStatusException e) {
                            processData.setProcessedStatus(BridgeUtil.STATUS_ERROR_UNREPEATABLE);
                            processData.setErrorMessage(e.getMessage());
                        } catch (Exception e) {
                            processData.setProcessedStatus(BridgeUtil.STATUS_ERROR);
                            processData.setErrorMessage(e.getMessage());

                        }
                    }
                    if (processData.getProcessedStatus() == BridgeUtil.STATUS_ERROR)
                        connection.rollback();
                    postProcess(connection, processData, schemaName);
                    connection.commit();
                }
            }
        } catch (Exception e) {
            throw new RawLoopRepoException(e);
        }
    }


    @Override
    public void preProcess(Connection connection, ProcessData processData, String schemaName) {

        try (CallableStatement prcPreProcess = connection.prepareCall(String.format("{ call %s.prc_pre_process(?,?,?,?,?,?,?) }", schemaName))) {

            prcPreProcess.setLong(1, processData.getRawId());
            prcPreProcess.setString(2, processData.getMetaData().getRawFullName());
            prcPreProcess.setString(3, processData.getMetaData().getBufFullName());
            prcPreProcess.setString(4, processData.getMetaData().getPrcExecFullName());
            prcPreProcess.setInt(5, processData.getProcessedStatus());
            prcPreProcess.setString(6, processData.getErrorMessage());
            prcPreProcess.setNull(7, Types.BIGINT);

            prcPreProcess.registerOutParameter(5, Types.INTEGER);
            prcPreProcess.registerOutParameter(6, Types.VARCHAR);
            prcPreProcess.registerOutParameter(7, Types.BIGINT);

            prcPreProcess.execute();

            processData.setProcessedStatus(prcPreProcess.getInt(5));
            processData.setErrorMessage(prcPreProcess.getString(6));
            processData.setBufId(prcPreProcess.getLong(7));
        } catch (Exception e) {
            throw new PreProcessRepoException(e);
        }
    }

    @Override
    public void postProcess(Connection connection, ProcessData processData, String schemaName) {

        try (CallableStatement prcPostProcess = connection.prepareCall(String.format("{ call %s.prc_post_process(?,?,?,?) }", schemaName))) {

            prcPostProcess.setLong(1, processData.getRawId());
            prcPostProcess.setString(2, processData.getMetaData().getRawFullName());
            prcPostProcess.setInt(3, processData.getProcessedStatus());
            prcPostProcess.setString(4, processData.getErrorMessage());

            prcPostProcess.execute();
        } catch (Exception e) {
            throw new PostProcessRepoException(e);
        }
    }


}
