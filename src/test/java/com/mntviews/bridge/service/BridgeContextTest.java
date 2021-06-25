package com.mntviews.bridge.service;

import com.mntviews.bridge.common.PostgresContainerUnitOld;
import com.mntviews.bridge.model.ConnectionData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
public class BridgeContextTest extends PostgresContainerUnitOld {

    final static String ERROR_LOG_TEXT = "Error";

    @Mock
    BridgeService bridgeService;


    @Test
    public void executeBridgeContextTest() {
        doNothing().when(bridgeService).execute(isA(String.class), isA(String.class), isNull(), isA(BridgeProcessing.class), isA(String.class));

        BridgeContext bridgeContext = BridgeContext
                .custom("GROUP_TAG", "META_TAG", new ConnectionData("URL", "USER_NAME", "PASSWORD", "DEFAULT_SCHEMA"))
                .withBridgeProcessing((connection, processData) -> {
                })
                .withBridgeService(bridgeService)
                .withDataBaseType(DataBaseType.TEST)
                .build();
        bridgeContext.execute();
    }

    @Test
    public void executeBridgeContext() {


        jdbcTemplate.update("DO $$" +
                "BEGIN " +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_payload) values (-1, 1, '{\"field\":\"test\"}');\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_payload) values (2, 2, '{\"field\":\"test2\"}');\n" +
                "END;" +
                "$$");

        bridgeContext.execute();

        jdbcTemplate.query("SELECT id,s_status,s_action,s_msg FROM " + SCHEMA_NAME + ".fbi_raw_" + META_TAG, new ResultSetExtractor<Void>() {
            @Override
            public Void extractData(ResultSet rs) throws SQLException, DataAccessException {
                while(rs.next()) {
                    if (rs.getLong(1) == -1) {
                        assertEquals(-3, rs.getLong(2));
                        assertEquals(0, rs.getLong(3));
                        assertEquals(ERROR_LOG_TEXT, rs.getString(4));
                    }

                    if (rs.getLong(1) == 1) {
                        assertEquals(1, rs.getLong(2));
                        assertEquals(1, rs.getLong(3));
                        assertEquals("", rs.getString(4));
                    }
                }
                return null;
            }
        });
    }


    @Test
    void executeBridgeContextMultiThread() throws Exception {
        jdbcTemplate.update("DO $$" +
                "BEGIN " +
                "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                "            LOOP\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_payload) values (-i,-i, '{\"field\":\"test' || i || '\"}');\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_payload) values (i,i, '{\"field\":\"test' || i || '\"}');\n" +
                "            END LOOP;" +
                "END;" +
                "$$");

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
            System.out.println("counterCount=" + counterCount);
        } catch (InterruptedException ex) {
            executor.shutdownNow();
            throw new Exception("Critical termination error");
        }


    }
}
