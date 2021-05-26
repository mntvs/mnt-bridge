package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.DataBaseInitService;
import lombok.RequiredArgsConstructor;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import java.sql.Connection;


@RequiredArgsConstructor
public class DataBaseInitPostgresqlServiceImpl implements DataBaseInitService {

    private final MetaInitRepo metaInitRepo;

    @Override
    public void migrate(ConnectionData connectionData, Boolean isClean) {
        Flyway flyway = Flyway.configure()
                .dataSource(connectionData.getUrl(), connectionData.getUserName(), connectionData.getPassword()).locations("classpath:db/migration/postgresql")
                .schemas(BridgeContext.DEFAULT_SCHEMA_NAME)
                .defaultSchema(BridgeContext.DEFAULT_SCHEMA_NAME).load();
        if (isClean)
            flyway.clean();
        flyway.migrate();

    }

    @Override
    public void migrate(ConnectionData connectionData) {
        migrate(connectionData, false);
    }

    @Override
    public void init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName) {
        metaInitRepo.init(metaInitRepo.getConnection(connectionData), groupTag, metaTag, schemaName, connectionData.getSchemaName());
    }

    @Override
    public Connection getConnection(ConnectionData connectionData) {
        return metaInitRepo.getConnection(connectionData);
    }
}
