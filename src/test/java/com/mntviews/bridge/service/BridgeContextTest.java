package com.mntviews.bridge.service;


import com.mntviews.bridge.common.BaseInit;
import com.mntviews.bridge.common.ContainerUnit;
import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.RawData;
import lombok.extern.java.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mntviews.bridge.common.ContainerUnit.*;
import static com.mntviews.bridge.service.BridgeUtil.*;
import static com.mntviews.bridge.service.BridgeUtil.STATUS_ERROR_UNREPEATABLE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;

@Log
@ExtendWith(MockitoExtension.class)
public class BridgeContextTest extends BaseInit {

    @Mock
    BridgeService bridgeService;

    @Test
    public void executeBridgeContextTest() {
        doNothing().when(bridgeService).execute(isA(MetaData.class), isNull(), isA(BridgeProcessing.class), isA(BridgeProcessing.class), isA(String.class), isNull());

        BridgeContext bridgeContext = BridgeContext
                .custom("GROUP_TAG", "META_TAG", new ConnectionData("URL", "USER_NAME", "PASSWORD", "DEFAULT_SCHEMA"))
                .withAfterProcessing((connection, processData) -> {
                })
                .withBeforeProcessing((connection, processData) -> {
                })
                .withBridgeService(bridgeService)
                .withDataBaseType(DataBaseType.TEST)
                .build();
        bridgeContext.init();
        bridgeContext.execute();
    }

    @Test
    public void executeBridgeContext() {

        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);
            JdbcTemplate jdbcTemplate = containerUnit.getJdbcTemplate();
            BridgeContext bridgeContext = containerUnit.getBridgeContext();
            jdbcTemplate.update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_payload) values (1, 1, '{\"field\":\"test\"}');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_payload) values (2, 2, '{\"field\":\"test2\"}');\n" +
                            "END;"));

            bridgeContext.execute();

            jdbcTemplate.query("SELECT id,s_status,s_action,s_msg FROM " + SCHEMA_NAME + ".fbi_raw_" + META_TAG, new ResultSetExtractor<Void>() {
                @Override
                public Void extractData(ResultSet rs) throws SQLException, DataAccessException {
                    while (rs.next()) {
                        if (rs.getLong(1) % 2 == 0) {
                            assertEquals(-3, rs.getLong(2));
                            assertEquals(0, rs.getLong(3));
                            assertEquals(ContainerUnit.TEST_EXCEPTION_TEXT, rs.getString(4));
                        }

                        if (rs.getLong(1) % 2 != 0) {
                            assertEquals(1, rs.getLong(2));
                            assertEquals(1, rs.getLong(3));
                            assertNull(rs.getString(4));
                        }
                    }
                    return null;
                }
            });

            int count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);

            assertEquals(count, 1);
        }
    }


    @Test
    void executeBridgeContextMultiThread() throws Exception {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);
            JdbcTemplate jdbcTemplate = containerUnit.getJdbcTemplate();
            BridgeContext bridgeContext = containerUnit.getBridgeContext();

            jdbcTemplate.update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                            "            LOOP\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, 'failed');\n" +
                            "            END LOOP;" +
                            "END;"));


            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(containerUnit.findTestProcedure("EXCEPTION2")));
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) Executors.newCachedThreadPool();

            AtomicInteger num = new AtomicInteger();
            Runnable startTaskProcedure = () -> {
                int taskNum = num.getAndIncrement();
                System.out.println("Start task " + taskNum);
                bridgeContext.execute();
                System.out.println("Finish task " + taskNum);
            };


            for (int i = 0; i < TASK_COUNT; i++) {
                executor.submit(startTaskProcedure);
            }

            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    throw new Exception("Time out error");
                }
                Integer errorCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3", Integer.class);
                assertEquals(ITEMS_COUNT, errorCount, "Error count raw");
                Integer successCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=1", Integer.class);
                assertEquals(ITEMS_COUNT, successCount, "Success count raw");
                Integer counterCount = jdbcTemplate.queryForObject("select sum(s_counter) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where  s_status=1", Integer.class);
                assertEquals(ITEMS_COUNT, counterCount, "Counter count raw");
                counterCount = jdbcTemplate.queryForObject("select sum(s_counter) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where  s_status=-3", Integer.class);

                int count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
                assertEquals(count, ITEMS_COUNT);
                System.out.println("counterCount=" + counterCount);
            } catch (InterruptedException ex) {
                executor.shutdownNow();
                throw new Exception("Critical termination error");
            }

        }
    }


    @Test
    void rawDataTest() throws SQLException {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);
            BridgeContext bridgeContext = containerUnit.getBridgeContext();
            RawData rawData = new RawData();
            rawData.setFId("1");
            rawData.setFPayload("test");
            Connection connection = bridgeContext.getConnection();
            bridgeContext.saveRawData(rawData, connection);
            rawData.setFPayload("test_edited");
            bridgeContext.saveRawData(rawData, connection);
            connection.commit();
            assertEquals("test", bridgeContext.findRawDataById(rawData.getId(), connection).getFPayload());
            bridgeContext.execute();
            assertNotNull(bridgeContext.findBufDataById(bridgeContext.findBufDataByRawId(rawData.getId(), connection).getId(), connection));
            connection.commit();
        }
    }


    @Test
    void checkOneRowTest() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);
            BridgeContext bridgeContext = containerUnit.getBridgeContext();

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (1, 'f_id_1');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (2, 'f_id_2');\n" +
                            "END;"));

            bridgeContext.executeOne(1L);
            Integer intactCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_INTACT + " and id=2", Integer.class);
            assertEquals(1, intactCount, dbTypeName + ": Row with intact status must be 1");

            Integer successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_SUCCESS + " and id=1", Integer.class);
            assertEquals(1, successCount, dbTypeName + ": Row with success status must be 1");
        }
    }


    @Test
    void checkOneGroupTest() {
        final String fGroupTestId = "group_test";

        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);
            BridgeContext bridgeContext = containerUnit.getBridgeContext();

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (1, 'f_id_1');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (2, 'f_id_2');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_group_id) values (3, 'f_id_1','" + fGroupTestId + "');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_group_id) values (5, 'f_id_2','" + fGroupTestId + "');\n" +
                            "END;"));

            bridgeContext.executeGroup(fGroupTestId);

            Integer intactCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_INTACT + " and f_group_id is null", Integer.class);
            assertEquals(2, intactCount, dbTypeName + ": Row with intact status must be 2");

            Integer successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_SUCCESS + " and f_group_id='" + fGroupTestId + "'", Integer.class);
            assertEquals(2, successCount, dbTypeName + ": Row with success status must be 2");

            successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG + " where f_group_id='" + fGroupTestId + "'", Integer.class);
            assertEquals(2, successCount, dbTypeName + ": Row with in buf table must be 2");
        }

        }


    @Test
    void checkAttemptParam() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);

            BridgeContext bridgeContextAttempt = containerUnit.getBridgeContextAttempt();
            bridgeContextAttempt.init();

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (1, 'f_id_1');\n" +
                            "END;"));

            bridgeContextAttempt.execute();

            Integer count = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR + " and s_action=0 and id=1", Integer.class);
            assertEquals(1, count, dbTypeName + ": Row with error status must be 1");

            bridgeContextAttempt.execute();

            count = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR_UNREPEATABLE + " and s_action=1 and id=1 and s_counter=2", Integer.class);
            assertEquals(1, count, dbTypeName + ": Row with unrepeatable error status must be 1");

        }
    }

}
