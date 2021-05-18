package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.ProcessData;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.exception.PostProcessRepoException;
import com.mntviews.bridge.repository.exception.PreProcessRepoException;
import com.mntviews.bridge.repository.exception.RawLoopRepoException;
import com.mntviews.bridge.service.BridgeProcessing;
import lombok.RequiredArgsConstructor;

import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor
public class RawLoopRepoImpl implements RawLoopRepo {


    @Override
    public void rawLoop(Connection connection, MetaData metaData, BridgeProcessing bridgeProcessing) {
        AtomicInteger count = new AtomicInteger();

        try (Statement stmt = connection.createStatement();) {
            try (ResultSet rs = stmt.executeQuery(metaData.getRawLoopQuery());) {
                while (rs.next()) {
                    Integer processedStatus = 0;
                    String errorMessage = null;
                    ProcessData processData = new ProcessData();
                    processData.setRawId(rs.getLong("id"));
                    processData.setMetaData(metaData);
                    processData.setProcessedStatus(processedStatus);
                    processData.setErrorMessage(errorMessage);
                    preProcess(connection, processData);
                    if (bridgeProcessing != null)
                        bridgeProcessing.process(connection, processData);
                    postProcess(connection, processData);
                    connection.commit();
                }
            }
        } catch (Exception e) {
            throw new RawLoopRepoException(e);
        }
    }


    @Override
    public void preProcess(Connection connection, ProcessData processData) {

        try (CallableStatement prcPreProcess = connection.prepareCall("{ call bridge.prc_pre_process(?,?,?,?,?,?) }");) {

            prcPreProcess.setLong(1, processData.getRawId());
            prcPreProcess.setString(2, processData.getMetaData().getRawFullName());
            prcPreProcess.setString(3, processData.getMetaData().getBufFullName());
            prcPreProcess.setString(4, processData.getMetaData().getPrcExecFullName());
            prcPreProcess.setInt(5, processData.getProcessedStatus());
            prcPreProcess.setString(6, processData.getErrorMessage());

            prcPreProcess.registerOutParameter(5, Types.INTEGER);
            prcPreProcess.registerOutParameter(6, Types.VARCHAR);

            prcPreProcess.execute();

            processData.setProcessedStatus(prcPreProcess.getInt(5));
            processData.setErrorMessage(prcPreProcess.getString(6));
        } catch (Exception e) {
            throw new PreProcessRepoException(e);
        }
    }

    @Override
    public void postProcess(Connection connection, ProcessData processData) {

        try (CallableStatement prcPostProcess = connection.prepareCall("{ call bridge.prc_post_process(?,?,?,?) }");) {

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
