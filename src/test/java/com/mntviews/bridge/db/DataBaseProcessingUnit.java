package com.mntviews.bridge.db;

import com.mntviews.bridge.common.BaseInit;
import com.mntviews.bridge.common.ContainerUnit;
import com.mntviews.bridge.service.BridgeContext;
import lombok.extern.java.Log;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mntviews.bridge.common.ContainerUnit.*;
import static com.mntviews.bridge.service.BridgeUtil.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Log
public class DataBaseProcessingUnit extends BaseInit {

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
            Integer successCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_SUCCESS, Integer.class);
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
            Integer errorCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR, Integer.class);
            assertEquals(ITEMS_COUNT, errorCount, dbTypeName + ": Error count raw");

            Integer errorCountBuf = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
            assertEquals(0, errorCountBuf, dbTypeName + ": Error count buf");

            /* Second pass */
            jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
            Integer errorCounter = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR + " and s_counter=2", Integer.class);
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
            Integer successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_SUCCESS, Integer.class);
            assertEquals(ITEMS_COUNT, successCount, dbTypeName + ": Success count raw");
            Integer errorCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR, Integer.class);
            assertEquals(ITEMS_COUNT, errorCount, dbTypeName + ": Error count raw");

            int count = containerUnit.getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
            assertEquals(count, ITEMS_COUNT);

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
                Integer successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_SUCCESS, Integer.class);
                assertEquals(ITEMS_COUNT, successCount, dbTypeName + ": Success count raw");
                Integer errorCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR + " and s_msg like '%" + TEST_EXCEPTION_TEXT + "%'", Integer.class);
                assertEquals(ITEMS_COUNT, errorCount, dbTypeName + ": Error count raw");
                Integer counterCount = containerUnit.getJdbcTemplate().queryForObject("select sum(s_counter) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where  s_status=" + STATUS_SUCCESS, Integer.class);
                assertEquals(ITEMS_COUNT, counterCount, dbTypeName + ": Counter count raw");
                counterCount = containerUnit.getJdbcTemplate().queryForObject("select sum(s_counter) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where  s_status=" + STATUS_ERROR, Integer.class);
                System.out.println("counterCount=" + counterCount);
            } catch (InterruptedException ex) {
                executor.shutdownNow();
                throw new Exception("Critical termination error");
            }
        }
    }

    @Test
    void checkCanceledRowTest() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            int id = 1;
            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_date, f_payload) values (" + id++ + ", 1, TO_DATE('02.01.2021','dd.mm.yyyy'),'test1');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_date, f_payload) values (" + id++ + ", 1, TO_DATE('02.01.2021','dd.mm.yyyy'),'test2');\n" +
                            "END;"));

            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_date) values (" + id++ + ", 1, TO_DATE('01.01.2021','dd.mm.yyyy') );\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_date) values (" + id++ + ", 1, TO_DATE('01.01.2021','dd.mm.yyyy') );\n" +
                            "END;"));

            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);

            Integer successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_SUCCESS + " and id in (2)", Integer.class);
            Integer canceledCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_CANCELED + " and id in (1,3,4)", Integer.class);

            assertEquals(1, successCount, dbTypeName + ": Row with success status must be 1");
            assertEquals(3, canceledCount, dbTypeName + ": Row with success canceled must be 3");

            Integer counterCount = containerUnit.getJdbcTemplate().queryForObject("select s_counter from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
            assertEquals(1, counterCount, dbTypeName + ": Buf counter must be 1");

            Long fRawId = containerUnit.getJdbcTemplate().queryForObject("select f_raw_id from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Long.class);
            assertEquals(2, fRawId, dbTypeName + ": f_raw_id must be equal 2");

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                update " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " set s_action=0 where id = 2;" +
                            "END;"));

            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);

            counterCount = containerUnit.getJdbcTemplate().queryForObject("select s_counter from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
            assertEquals(2, counterCount, dbTypeName + ": Buf counter should be 2 after update");


        }
    }

    @Test
    void checkOneRowTest() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (1, 'f_id_1');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (2, 'f_id_2');\n" +
                            "END;"));
            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG, 1);

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
            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (1, 'f_id_1');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (2, 'f_id_2');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_group_id) values (3, 'f_id_1','" + fGroupTestId + "');\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_group_id) values (4, 'f_id_2','" + fGroupTestId + "');\n" +
                            "END;"));
            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?,?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG, null, fGroupTestId);

            Integer intactCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_INTACT + " and f_group_id is null", Integer.class);
            assertEquals(2, intactCount, dbTypeName + ": Row with intact status must be 2");

            Integer successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_SUCCESS + " and f_group_id='" + fGroupTestId + "'", Integer.class);
            assertEquals(2, successCount, dbTypeName + ": Row with success status must be 2");

            successCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG + " where f_group_id='" + fGroupTestId + "'", Integer.class);
            assertEquals(2, successCount, dbTypeName + ": Row with in buf table must be 2");
        }
    }

    @Test
    void checkNotRepeatableException() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (1, 'f_id_1');\n" +
                            "END;"));

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(containerUnit.findTestProcedure("EXCEPTION3")));

            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG, 1);

            Integer intactCount = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR_UNREPEATABLE + " and s_action=1 and id=1", Integer.class);
            assertEquals(1, intactCount, dbTypeName + ": Row with unrepeatable status must be 1");

        }
    }

    @Test
    void checkAttempParam() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            " update " + BridgeContext.DEFAULT_SCHEMA_NAME + ".bridge_meta set param= '" + containerUnit.attemptTestParam + "' where tag='" + META_TAG + "';" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id) values (1, 'f_id_1');\n" +
                            "END;"));

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(containerUnit.findTestProcedure("EXCEPTION")));

            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG, 1);

            Integer count = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR + " and s_action=0 and id=1", Integer.class);
            assertEquals(1, count, dbTypeName + ": Row with error status must be 1");

            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG, 1);

            count = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR_UNREPEATABLE + " and s_action=1 and id=1", Integer.class);
            assertEquals(1, count, dbTypeName + ": Row with unrepeatable status must be 1");

        }
    }


    @Test
    void checkSkipParam() {
        for (ContainerUnit containerUnit : containerUnitList) {
            String dbTypeName = containerUnit.findDbTypeName();
            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(
                    "BEGIN " +
                            " update " + BridgeContext.DEFAULT_SCHEMA_NAME + ".bridge_meta set param= '" + containerUnit.skipTestParam + "' where tag='" + META_TAG + "';" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_date) values (1, 'f_id_1', to_date('01.01.2022','dd.mm.yyyy'));\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_date) values (2, 'f_id_1', to_date('02.01.2022','dd.mm.yyyy'));\n" +
                            "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (id, f_id, f_date) values (3, 'f_id_1', to_date('02.01.2022','dd.mm.yyyy'));\n" +
                            "END;"));

            containerUnit.getJdbcTemplate().update(containerUnit.wrapCodeBlock(containerUnit.findTestProcedure("EXCEPTION")));

            containerUnit.getJdbcTemplate().update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);

            Integer count = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_CANCELED + " and s_action=1 and id=1", Integer.class);
            assertEquals(1, count, dbTypeName + ": id=1 must be skipped");

            count = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_CANCELED + " and s_action=1 and id=2", Integer.class);
            assertEquals(1, count, dbTypeName + ": id=2 must be skipped");

            count = containerUnit.getJdbcTemplate().queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=" + STATUS_ERROR + " and s_action=0 and id=3", Integer.class);
            assertEquals(1, count, dbTypeName + ": id=3 must be error");

        }
    }

}
