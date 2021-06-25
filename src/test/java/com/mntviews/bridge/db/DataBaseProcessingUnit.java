package com.mntviews.bridge.db;

import com.mntviews.bridge.common.ContainerUnit;
import com.mntviews.bridge.common.OracleContainerUnit;
import com.mntviews.bridge.service.BridgeContext;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataBaseProcessingUnit {
    private List<ContainerUnit> containerUnitList;

    public DataBaseProcessingUnit() {
        containerUnitList = new ArrayList<>();
        containerUnitList.add(new OracleContainerUnit());
    }

    @BeforeEach
    public void init() {
        containerUnitList = new ArrayList<>();
        OracleContainerUnit oracleContainerUnit = new OracleContainerUnit();
        oracleContainerUnit.getBridgeContext().migrate(true);
        oracleContainerUnit.getBridgeContext().init();
        containerUnitList.add(new OracleContainerUnit());
    }

    @AfterEach
    public void clear() {
        containerUnitList.forEach(containerUnit -> containerUnit.getBridgeContext().clear());
    }

    @Test
    void startTaskAllSuccessTest() {
        for (ContainerUnit containerUnit : containerUnitList) {
            JdbcTemplate jdbcTemplate = containerUnit.getJdbcTemplate();
            jdbcTemplate.update("DO $$" +
                    "BEGIN " +
                    "    FOR i IN 1.." + containerUnit.ITEMS_COUNT + "\n" +
                    "            LOOP\n" +
                    "                insert into " + containerUnit.SCHEMA_NAME + ".fbi_raw_" + containerUnit.META_TAG + " (f_id, f_payload) values (i, '{\"field\":\"test' || i || '\"}');\n" +
                    "            END LOOP;" +
                    "END;" +
                    "$$");

            Integer notProcessedCount = jdbcTemplate.queryForObject("select count(*) from " + containerUnit.SCHEMA_NAME + ".fbi_raw_" + containerUnit.META_TAG + " where s_status=0", Integer.class);
            assertEquals(containerUnit.ITEMS_COUNT, notProcessedCount, "not processed count");
            jdbcTemplate.update(String.format("call %s.prc_start_task(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), containerUnit.GROUP_TAG, containerUnit.META_TAG);
            Integer successCount = jdbcTemplate.queryForObject("select count(*) from " + containerUnit.SCHEMA_NAME + ".fbi_raw_" + containerUnit.META_TAG + " where s_status=1", Integer.class);
            assertEquals(containerUnit.ITEMS_COUNT, successCount, "Success count raw");
            Integer successCountBuf = jdbcTemplate.queryForObject("select count(*) from " + containerUnit.SCHEMA_NAME + ".fbi_buf_" + containerUnit.META_TAG, Integer.class);
            assertEquals(containerUnit.ITEMS_COUNT, successCountBuf, "Success count buf");
        }
    }
}
