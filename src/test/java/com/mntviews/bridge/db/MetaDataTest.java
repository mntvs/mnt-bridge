package com.mntviews.bridge.db;

import com.mntviews.bridge.common.PostgresContainerUnitOld;
import com.mntviews.bridge.service.BridgeContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Creates test objects and checks meta data structure
 */
public class MetaDataTest extends PostgresContainerUnitOld {

    @Test
    void startTaskAllSuccessTest() {
        jdbcTemplate.update("DO $$" +
                "BEGIN " +
                "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                "            LOOP\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                "            END LOOP;" +
                "END;" +
                "$$");

        Integer notProcessedCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=0", Integer.class);
        assertEquals(ITEMS_COUNT, notProcessedCount, "not processed count");
        jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
        Integer successCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=1", Integer.class);
        assertEquals(ITEMS_COUNT, successCount, "Success count raw");
        Integer successCountBuf = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
        assertEquals(ITEMS_COUNT, successCountBuf, "Success count buf");


    }

    @Test
    void startTaskAllErrorTest() {
        jdbcTemplate.update("DO $$" +
                "BEGIN " +
                "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                "            LOOP\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, 'failed json');\n" +
                "            END LOOP;" +
                "END;" +
                "$$");

        /* First pass */
        jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
        Integer errorCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3", Integer.class);
        assertEquals(ITEMS_COUNT, errorCount, "Error count raw");

        Integer errorCountBuf = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
        assertEquals(0, errorCountBuf, "Error count buf");

        /* Second pass */
        jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
        Integer errorCounter = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3 and s_counter=2", Integer.class);
        assertEquals(ITEMS_COUNT, errorCounter, "Error counter should be equal 2 ");

    }

    @Test
    void startTaskMixedTest() {
        jdbcTemplate.update("DO $$" +
                "BEGIN " +
                "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                "            LOOP\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, 'failed json');\n" +
                "            END LOOP;" +
                "END;" +
                "$$");

        jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
        Integer successCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=1", Integer.class);
        assertEquals(ITEMS_COUNT, successCount, "Success count raw");
        Integer errorCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3", Integer.class);
        assertEquals(ITEMS_COUNT, errorCount, "Error count raw");
    }

    @Test
    public void multiThreadStartTask() throws Exception {

        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newCachedThreadPool();

        jdbcTemplate.update("DO $$" +
                "BEGIN " +
                "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                "            LOOP\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, 'failed json');\n" +
                "            END LOOP;" +
                "END;" +
                "$$");

        AtomicInteger num = new AtomicInteger();
        Runnable startTaskProcedure = () -> {
            int taskNum = num.getAndIncrement();
            System.out.println("Start task " + taskNum);
            jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
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
            Integer successCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=1", Integer.class);
            assertEquals(ITEMS_COUNT, successCount, "Success count raw");
            Integer errorCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=-3", Integer.class);
            assertEquals(ITEMS_COUNT, errorCount, "Error count raw");
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
