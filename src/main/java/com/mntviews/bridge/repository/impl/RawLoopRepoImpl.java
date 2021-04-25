package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.ProcessData;
import com.mntviews.bridge.repository.RawLoopRepo;
import com.mntviews.bridge.repository.exception.PostProcessRepoException;
import com.mntviews.bridge.repository.exception.PreProcessRepoException;
import com.mntviews.bridge.repository.exception.RawLoopRepoException;
import com.mntviews.bridge.service.BridgeProcessing;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor
public class RawLoopRepoImpl implements RawLoopRepo {

    private final JdbcTemplate jdbcTemplate;

    public void rawLoopDel(Connection connection, MetaData metaData) {
        AtomicInteger count = new AtomicInteger();
        ResultSetExtractor<Boolean> rse = rs -> {
            while (rs.next()) {
                Integer processedStatus = 0;
                String errorMessage = null;
                ProcessData processData = new ProcessData();
                processData.setRawId(rs.getLong("id"));
                processData.setMetaData(metaData);
                processData.setProcessedStatus(processedStatus);
                processData.setErrorMessage(errorMessage);
                preProcess(connection, processData);

                postProcess(connection, processData);

                if (processData.getProcessedStatus() != 0) {
                    count.getAndIncrement();
                }

                jdbcTemplate.getDataSource().getConnection().commit();
                System.out.println(rs.getInt("id"));
            }
            return null;
        };

        jdbcTemplate.query(metaData.getRawLoopQuery(), rse);

    }

    @Override
    public void rawLoop(Connection connection, MetaData metaData, BridgeProcessing bridgeProcessing) {
        AtomicInteger count = new AtomicInteger();

        try {
            Statement stmt = connection.createStatement();
            try {
                ResultSet rs = stmt.executeQuery(metaData.getRawLoopQuery());

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
            } finally {
                stmt.close();
            }
        } catch (Exception e) {
            throw new RawLoopRepoException(e);
        }


  /*      ResultSetExtractor<Boolean> rse = rs -> {
            while (rs.next()) {
                Integer processedStatus = 0;
                String errorMessage = null;
                ProcessData processData = new ProcessData();
                processData.setRawId(rs.getLong("id"));
                processData.setMetaData(metaData);
                processData.setProcessedStatus(processedStatus);
                processData.setErrorMessage(errorMessage);
                preProcess(processData);

                postProcess(processData);

                if (processData.getProcessedStatus() != 0) {
                    count.getAndIncrement();
                }

                jdbcTemplate.getDataSource().getConnection().commit();
                System.out.println(rs.getInt("id"));
            }
            return null;
        };

        jdbcTemplate.query(metaData.getRawLoopQuery(), rse);
*/
    }


    public void preProcessDel(ProcessData processData) {

        /*Statement st = con.createStatement();
        ResultSet rs = st.executeQuery("CALL my_proc('whatever')");
        rs.next();
        */
        SimpleJdbcCall jdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withSchemaName("bridge")
                .withProcedureName("prc_pre_process").declareParameters(new SqlParameter("a_raw_id", Types.BIGINT),
                        new SqlParameter("a_raw_full_name", Types.VARCHAR),
                        new SqlParameter("a_buf_full_name", Types.VARCHAR),
                        new SqlParameter("prc_exec_full_name", Types.VARCHAR),
                        new SqlInOutParameter("a_processed_status", Types.INTEGER),
                        new SqlInOutParameter("a_error_message", Types.VARCHAR)
                );

        Map<String, Object> inParamMap = new HashMap<String, Object>();
        inParamMap.put("a_raw_id", processData.getRawId());
        inParamMap.put("a_raw_full_name", processData.getMetaData().getRawFullName());
        inParamMap.put("a_buf_full_name", processData.getMetaData().getBufFullName());
        inParamMap.put("a_prc_exec_full_name", processData.getMetaData().getPrcExecFullName());
        inParamMap.put("a_processed_status", processData.getProcessedStatus());
        inParamMap.put("a_error_message", processData.getErrorMessage());
        SqlParameterSource in = new MapSqlParameterSource(inParamMap);


        Map<String, Object> simpleJdbcCallResult = jdbcCall.execute(in);
        processData.setProcessedStatus((Integer) simpleJdbcCallResult.get("a_processed_status"));
        processData.setErrorMessage((String) simpleJdbcCallResult.get("a_error_message"));

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

    public void postProcessDel(ProcessData processData) {

        /*Statement st = con.createStatement();
        ResultSet rs = st.executeQuery("CALL my_proc('whatever')");
        rs.next();
        */
        SimpleJdbcCall jdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withSchemaName("bridge")
                .withProcedureName("prc_post_process").declareParameters(new SqlParameter("a_raw_id", Types.BIGINT),
                        new SqlParameter("a_raw_full_name", Types.VARCHAR),
                        new SqlParameter("a_processed_status", Types.INTEGER),
                        new SqlParameter("a_error_message", Types.VARCHAR)
                );

        Map<String, Object> inParamMap = new HashMap<String, Object>();
        inParamMap.put("a_raw_id", processData.getRawId());
        inParamMap.put("a_raw_full_name", processData.getMetaData().getRawFullName());
        inParamMap.put("a_processed_status", processData.getProcessedStatus());
        inParamMap.put("a_error_message", processData.getErrorMessage());
        SqlParameterSource in = new MapSqlParameterSource(inParamMap);

        jdbcCall.execute(in);

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
