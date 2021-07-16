package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;


public class DataBaseInitOracleServiceImpl  extends DataBaseInit {

    private final static String DDL_CREATE_PATH = "db/oracle/ddl_create.sql";
    private final static String DDL_DROP_PATH = "db/oracle/ddl_drop.sql";

    public DataBaseInitOracleServiceImpl(MetaInitRepo metaInitRepo, MetaDataRepo metaDataRepo) {
        super(metaInitRepo, metaDataRepo);
    }


    public DataBaseInitOracleServiceImpl(MetaInitRepo metaInitRepo) {
        super(metaInitRepo, new MetaDataRepoImpl());
    }

    @Override
    public void migrate(ConnectionData connectionData, Boolean isClean) {
        migrate(connectionData, isClean, DDL_CREATE_PATH, DDL_DROP_PATH);
    }
}
