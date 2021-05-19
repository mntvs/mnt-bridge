package com.mntviews.bridge.repository;

import com.mntviews.bridge.common.PostgresContainerTest;
import com.mntviews.bridge.repository.impl.MetaInitPostgresqlRepoImpl;
import com.mntviews.bridge.service.BridgeContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetaInitRepoTest extends PostgresContainerTest {

    private final static String GROUP_TAG_NEW = GROUP_TAG + "_NEW";
    private final static String META_TAG_NEW = META_TAG + "_NEW";

    private MetaInitRepo metaInitRepo;

    @Test
    public void initTest() {
        metaInitRepo = new MetaInitPostgresqlRepoImpl();
        metaInitRepo.init(connection, GROUP_TAG, META_TAG, SCHEMA_NAME, BridgeContext.DEFAULT_SCHEMA_NAME);
        Integer count = jdbcTemplate.queryForObject(String.format("select count(*) c from %s.bridge_group where tag=?", BridgeContext.DEFAULT_SCHEMA_NAME), Integer.class, GROUP_TAG);
        assertEquals(count, 1, String.format("Group %s should be created", GROUP_TAG));
        count = jdbcTemplate.queryForObject(String.format("select count(*) c from %s.bridge_meta where tag=?", BridgeContext.DEFAULT_SCHEMA_NAME), Integer.class, META_TAG);
        assertEquals(count, 1, String.format("Meta %s should be created", GROUP_TAG));

        metaInitRepo.init(connection, GROUP_TAG_NEW, META_TAG_NEW, SCHEMA_NAME, BridgeContext.DEFAULT_SCHEMA_NAME);
        count = jdbcTemplate.queryForObject(String.format("select count(*) c from %s.bridge_group where tag=?", BridgeContext.DEFAULT_SCHEMA_NAME), Integer.class, GROUP_TAG_NEW);
        assertEquals(count, 1, String.format("Group %s should be created", GROUP_TAG_NEW));
        count = jdbcTemplate.queryForObject(String.format("select count(*) c from %s.bridge_meta where tag=?", BridgeContext.DEFAULT_SCHEMA_NAME), Integer.class, META_TAG_NEW);
        assertEquals(count, 1, String.format("Meta %s should be created", META_TAG_NEW));
    }

}
