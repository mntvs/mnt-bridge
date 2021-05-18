package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.FlyWayService;
import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

public class FlyWayServicePostgresqlImpl implements FlyWayService {
    @Override
    public void migrate(ConnectionData connectionData) {
        Flyway.configure()
                .dataSource(connectionData.getUrl(), connectionData.getUserName(), connectionData.getPassword()).locations("classpath:db/migration/postgresql")
                .schemas(BridgeContext.DEFAULT_SCHEMA_NAME)
                .defaultSchema(BridgeContext.DEFAULT_SCHEMA_NAME)
                .load()
                .migrate();
    }
}
