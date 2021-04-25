package com.mntviews.bridge.repository;

import com.mntviews.bridge.common.PostgresContainerTest;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MetaDataRepoTest extends PostgresContainerTest {

    private MetaDataRepo metaDataRepo;

    @Test
    public void findMetaDataTest() {
        metaDataRepo = new MetaDataRepoImpl(jdbcTemplate);
        MetaData metaData = metaDataRepo.findMetaData(connection, GROUP_TAG, META_TAG);
        assertNotNull(metaData);
    }
}
