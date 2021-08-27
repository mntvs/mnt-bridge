package com.mntviews.bridge.service;


import com.mntviews.bridge.common.ContainerUnit;
import com.mntviews.bridge.common.OracleContainerUnit;
import com.mntviews.bridge.common.PostgresContainerUnit;
import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.RawData;
import lombok.extern.java.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mntviews.bridge.common.ContainerUnit.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;

@Log
@ExtendWith(MockitoExtension.class)
public class BridgeContextTest {

    @Mock
    BridgeService bridgeService;

    private List<ContainerUnit> containerUnitList;

    @BeforeEach
    public void init() {

        containerUnitList = new ArrayList<>();
        log.info("OracleContainerUnit init");
        OracleContainerUnit oracleContainerUnit = new OracleContainerUnit();
        oracleContainerUnit.getBridgeContext().migrate(true);
        oracleContainerUnit.getBridgeContext().clear();
        oracleContainerUnit.getBridgeContext().init();

        containerUnitList.add(oracleContainerUnit);

        log.info("PostgresContainerUnit init");
        PostgresContainerUnit postgresContainerUnit = new PostgresContainerUnit();
        postgresContainerUnit.getBridgeContext().migrate(true);
        postgresContainerUnit.getBridgeContext().clear();
        postgresContainerUnit.getBridgeContext().init();
        containerUnitList.add(postgresContainerUnit);

    }

    @AfterEach
    public void clear() {

        containerUnitList.forEach(containerUnit -> containerUnit.getBridgeContext().clear());
    }


    @Test
    public void executeBridgeContextTest() {
        doNothing().when(bridgeService).execute(isA(MetaData.class), isNull(), isA(BridgeProcessing.class), isA(String.class));

        BridgeContext bridgeContext = BridgeContext
                .custom("GROUP_TAG", "META_TAG", new ConnectionData("URL", "USER_NAME", "PASSWORD", "DEFAULT_SCHEMA"))
                .withBridgeProcessing((connection, processData) -> {

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
                        if (rs.getLong(1) %2 == 0) {
                            assertEquals(-3, rs.getLong(2));
                            assertEquals(0, rs.getLong(3));
                            assertEquals(ContainerUnit.TEST_EXCEPTION_TEXT, rs.getString(4));
                        }

                        if (rs.getLong(1) %2 != 0) {
                            assertEquals(1, rs.getLong(2));
                            assertEquals(1, rs.getLong(3));
                            assertNull(rs.getString(4));
                        }
                    }
                    return null;
                }
            });

            int count=jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);

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

                int count=jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
                assertEquals(count, ITEMS_COUNT);
                System.out.println("counterCount=" + counterCount);
            } catch (InterruptedException ex) {
                executor.shutdownNow();
                throw new Exception("Critical termination error");
            }

        }
    }


    @Test
    void rawDataTest() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);
            BridgeContext bridgeContext = containerUnit.getBridgeContext();
            RawData rawData = new RawData();
            rawData.setFId("1");
            rawData.setFPayload("test");
            bridgeContext.saveRawData(rawData);
            rawData.setFPayload("test_edited");
            bridgeContext.saveRawData(rawData);
            assertEquals("test",bridgeContext.findRawDataById(rawData.getId()).getFPayload());
            bridgeContext.execute();
            assertNotNull(bridgeContext.findBufDataById(bridgeContext.findBufDataByRawId(rawData.getId()).getId()));
        }
    }
}
