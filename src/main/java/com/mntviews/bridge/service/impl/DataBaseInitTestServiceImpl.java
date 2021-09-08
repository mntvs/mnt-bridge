package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.repository.impl.MetaDataRepoImpl;
import com.mntviews.bridge.service.DataBaseInitService;

import java.sql.Connection;

public class DataBaseInitTestServiceImpl extends DataBaseInit {

    public DataBaseInitTestServiceImpl(MetaInitRepo metaInitRepo, MetaDataRepo metaDataRepo) {
        super(metaInitRepo, metaDataRepo);
    }

    public DataBaseInitTestServiceImpl(MetaInitRepo metaInitRepo) {
        super(metaInitRepo, new MetaDataRepoImpl());
    }

    @Override
    public void migrate(ConnectionData connectionData, Boolean isClean) {

    }

    @Override
    public MetaData init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName, String param) {
        return new MetaData();
    }

}