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
import com.mntviews.bridge.service.ParamEnum;
import lombok.RequiredArgsConstructor;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class RawLoopRepoImpl implements RawLoopRepo {

    private void process(ProcessData processData, BridgeProcessing bridgeProcessing, Connection connection) {
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


    /**
     * Main loop to process raw queue
     * TODO: Modify to start with only provided raw_id
     * TODO: Option to change order
     *
     * @param connection             opened connection with db
     * @param metaData               system data received from db
     * @param beforeProcessing outer procedure to process current raw before db process
     * @param schemaName             schema name for system system objects
     * @param rawId
     * @param param
     */
    @Override
    public void rawLoop(Connection connection, MetaData metaData, BridgeProcessing beforeProcessing, BridgeProcessing afterProcessing, String schemaName, Long rawId, Map<String, Object> param) {

        Map<String, Object> localParam = new HashMap<>(metaData.getParam());
        if (param != null)
            localParam.putAll(param);
        String rawLoopQuery;
        final String paramMessage = "parameter '";
        final String selectQuery = "select id, s_counter from " + metaData.getRawFullName();
        if (rawId == null) {
            Object paramOrder = localParam.get(ParamEnum.ORDER.name());

            if (paramOrder == null)
                throw new RawLoopRepoException(paramMessage + ParamEnum.ORDER.name() + "' is not defined");

            if (!(paramOrder instanceof String))
                throw new RawLoopRepoException(paramMessage + ParamEnum.ORDER.name() + "' is must be String");

            switch ((String) paramOrder) {
                case "LIFO":
                    rawLoopQuery = selectQuery + " where s_action=0 order by s_date desc, id desc";
                    break;
                case "FIFO":
                    rawLoopQuery = selectQuery + " where s_action=0 order by s_date asc, id asc";
                    break;
                default:
                    throw new RawLoopRepoException("Parameter '" + BridgeUtil.PARAM_ORDER + "' must be FIFO or LIFO");
            }

        } else
            rawLoopQuery = selectQuery + " where s_action=0 and id=?";

        Object paramAttempt = localParam.get(ParamEnum.ATTEMPT.name());
        if (paramAttempt == null)
            throw new RawLoopRepoException(paramMessage + ParamEnum.ATTEMPT.name() + "' is not defined");

        if (paramAttempt instanceof String)
            paramAttempt = Integer.valueOf(String.valueOf(paramAttempt));

        if (!(paramAttempt instanceof Integer))
            throw new RawLoopRepoException(paramMessage + ParamEnum.ATTEMPT.name() + "' is must be Integer");


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

                    if (processData.getProcessedStatus() == BridgeUtil.STATUS_SUCCESS) {
                        process(processData, beforeProcessing, connection);
                    }
                    if (processData.getProcessedStatus() == BridgeUtil.STATUS_SUCCESS)
                        process(connection, processData, schemaName);

                    if (processData.getProcessedStatus() == BridgeUtil.STATUS_SUCCESS) {
                        process(processData, afterProcessing, connection);
                    }
                    if (processData.getProcessedStatus() != BridgeUtil.STATUS_SUCCESS)
                        connection.rollback();

                    if ((Integer) paramAttempt != -1 && rs.getInt("s_counter") + 1 >= (Integer) paramAttempt && processData.getProcessedStatus() == BridgeUtil.STATUS_ERROR) {
                        processData.setProcessedStatus(BridgeUtil.STATUS_ERROR_UNREPEATABLE);
                    }

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

    @Override
    public void process(Connection connection, ProcessData processData, String schemaName) {
        try (CallableStatement prcProcess = connection.prepareCall(String.format("{ call %s.prc_process(?,?,?,?,?) }", schemaName))) {

            prcProcess.setLong(1, processData.getRawId());
            prcProcess.setLong(2, processData.getBufId());
            prcProcess.setString(3, processData.getMetaData().getPrcExecFullName());
            prcProcess.setInt(4, processData.getProcessedStatus());
            prcProcess.setString(5, processData.getErrorMessage());
            prcProcess.registerOutParameter(4, Types.INTEGER);
            prcProcess.registerOutParameter(5, Types.VARCHAR);

            prcProcess.execute();
            processData.setProcessedStatus(prcProcess.getInt(4));
            processData.setErrorMessage(prcProcess.getString(5));
        } catch (Exception e) {
            throw new PostProcessRepoException(e);
        }
    }


}
