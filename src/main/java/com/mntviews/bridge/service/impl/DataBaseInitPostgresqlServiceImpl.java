package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.service.DataBaseInitService;
import com.mntviews.bridge.service.ScriptRunner;
import com.mntviews.bridge.service.exception.DataBaseInitServiceException;
import lombok.RequiredArgsConstructor;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;


public class DataBaseInitPostgresqlServiceImpl extends DataBaseInit {

    private final static String DDL_CREATE_PATH = "db/postgresql/ddl_create.sql";
    private final static String DDL_DROP_PATH = "db/postgresql/ddl_drop.sql";

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
}
