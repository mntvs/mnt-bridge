package com.mntviews.bridge.repository;

import com.mntviews.bridge.common.PostgresContainerTest;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.repository.impl.RawLoopRepoImpl;
import com.mntviews.bridge.service.BridgeContext;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RawLoopRepoTest  extends PostgresContainerTest {

    @Test
    public void rowLoopTest() throws SQLException {

        MetaDataRepo metaDataRepo = new MetaDataRepoImpl();
        RawLoopRepo rawLoopRepo = new RawLoopRepoImpl();
        MetaData metaData = metaDataRepo.findMetaData(connection, GROUP_TAG, META_TAG, BridgeContext.DEFAULT_SCHEMA_NAME);
        assertNotNull(metaData);

        jdbcTemplate.update("DO $$" +
                "BEGIN " +
                "    FOR i IN 1.." + ITEMS_COUNT + "\n" +
                "            LOOP\n" +
                "                insert into " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                "            END LOOP;" +
                "END;" +
                "$$");

        //rawLoopRepo.rawLoop(jdbcTemplate.getDataSource().getConnection(),metaData);
        rawLoopRepo.rawLoop(connection,metaData,null);
        Integer successCount = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_raw_" + META_TAG + " where s_status=1", Integer.class);
        assertEquals(ITEMS_COUNT, successCount, "Success count raw");
        Integer successCountBuf = jdbcTemplate.queryForObject("select count(*) from " + SCHEMA_NAME + ".fbi_buf_" + META_TAG, Integer.class);
        assertEquals(ITEMS_COUNT, successCountBuf, "Success count buf");

    }
}
