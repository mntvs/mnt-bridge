package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.service.ParamTypeEnum;

import java.util.Map;

public class DataBaseInitPostgresqlServiceImpl extends DataBaseInit {

    private static final String DDL_CREATE_PATH = "db/postgresql/ddl_create.sql";
    private static final String DDL_DROP_PATH = "db/postgresql/ddl_drop.sql";

    public DataBaseInitPostgresqlServiceImpl(MetaInitRepo metaInitRepo, MetaDataRepo metaDataRepo) {
        super(metaInitRepo, metaDataRepo);
    }

    public DataBaseInitPostgresqlServiceImpl(MetaInitRepo metaInitRepo) {
        super(metaInitRepo, new MetaDataRepoImpl());
    }


    @Override
    public void migrate(ConnectionData connectionData, Boolean isClean) {
        migrate(connectionData, isClean, DDL_CREATE_PATH, DDL_DROP_PATH);
    }
    @Override
    public MetaData init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName, Map<String, Object> param) {
        return init(connectionData, groupTag, metaTag, schemaName, ParamTypeEnum.JSON.toString(param));
    }
}
