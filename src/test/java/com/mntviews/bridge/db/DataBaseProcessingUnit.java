package com.mntviews.bridge.db;

import com.mntviews.bridge.common.ContainerUnit;
import com.mntviews.bridge.common.OracleContainerUnit;
import com.mntviews.bridge.common.PostgresContainerUnit;
import com.mntviews.bridge.service.BridgeContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mntviews.bridge.common.ContainerUnit.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Log
public class DataBaseProcessingUnit {
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
    void startTaskAllSuccessTest() {
        log.info("startTaskAllSuccessTest");
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);
            JdbcTemplate jdbcTemplate = containerUnit.getJdbcTemplate();

            jdbcTemplate.update(
                    containerUnit.wrapCodeBlock("BEGIN " +
                            "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                            "            LOOP\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                            "            END LOOP;" +
                            "END;"));

            Integer notProcessedCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=0", Integer.class);
            assertEquals(ITEMS_COUNT, notProcessedCount, dbTypeName + ": not processed count");
            jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
            Integer successCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=1", Integer.class);
            assertEquals(ITEMS_COUNT, successCount, dbTypeName + ": Success count raw");
            Integer successCountBuf = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
            assertEquals(ITEMS_COUNT, successCountBuf, dbTypeName + ": Success count buf");

        }
    }

    @Test
    void startTaskAllErrorTest() throws SQLException {
        log.info("startTaskAllErrorTest");
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            JdbcTemplate jdbcTemplate = containerUnit.getJdbcTemplate();

            jdbcTemplate.update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                            "            LOOP\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                            "            END LOOP;" +
                            "END;"));

            jdbcTemplate.update(containerUnit.wrapCodeBlock(containerUnit.findTestProcedure("EXCEPTION")));

            /* First pass */
            jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
            Integer errorCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3", Integer.class);
            assertEquals(ITEMS_COUNT, errorCount, dbTypeName + ": Error count raw");

            Integer errorCountBuf = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
            assertEquals(0, errorCountBuf, dbTypeName + ": Error count buf");

            /* Second pass */
            jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
            Integer errorCounter = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3 and s_counter=2", Integer.class);
            assertEquals(ITEMS_COUNT, errorCounter, dbTypeName + ": Error counter should be equal 2 ");
        }
    }


    @Test
    void startTaskMixedTest() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            log.info(dbTypeName);
            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                            "            LOOP\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, 'failed json');\n" +
                            "            END LOOP;" +
                            "END;"));


            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(containerUnit.findTestProcedure("EXCEPTION2")));

            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
            Integer successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=1", Integer.class);
            assertEquals(ITEMS_COUNT, successCount,dbTypeName + ": Success count raw");
            Integer errorCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3", Integer.class);
            assertEquals(ITEMS_COUNT, errorCount, dbTypeName + ": Error count raw");
        }
    }

    @Test
    public void multiThreadStartTask() throws Exception {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();

            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) Executors.newCachedThreadPool();

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                    "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                    "            LOOP\n" +
                    "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                    "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, 'failed json');\n" +
                    "            END LOOP;" +
                    "END;"));

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(containerUnit.findTestProcedure("EXCEPTION2")));

            AtomicInteger num = new AtomicInteger();
            Runnable startTaskProcedure = () -> {
                int taskNum = num.getAndIncrement();
                System.out.println("Start task " + taskNum);
                containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
                System.out.println("Finish task " + taskNum);
            };

            for (int i = 0; i < TASK_COUNT; i++) {
                executor.submit(startTaskProcedure);
            }

           executor.shutdown();
            try {
                if (!executor.awaitTermination(200, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    throw new Exception("Time out error");
                }
                Integer successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=1", Integer.class);
                assertEquals(ITEMS_COUNT, successCount, dbTypeName + ": Success count raw");
                Integer errorCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3 and s_msg like '%" + TEST_EXCEPTION_TEXT + "%'", Integer.class);
                assertEquals(ITEMS_COUNT, errorCount, dbTypeName + ": Error count raw");
                Integer counterCount = containerUnit.getJdbcTemplate().queryForObject("select sum(s_counter) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where  s_status=1", Integer.class);
                assertEquals(ITEMS_COUNT, counterCount,dbTypeName + ": Counter count raw");
                counterCount = containerUnit.getJdbcTemplate().queryForObject("select sum(s_counter) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where  s_status=-3", Integer.class);
                System.out.println("counterCount=" + counterCount);
            } catch (InterruptedException ex) {
                executor.shutdownNow();
                throw new Exception("Critical termination error");
            }
        }
    }

}
