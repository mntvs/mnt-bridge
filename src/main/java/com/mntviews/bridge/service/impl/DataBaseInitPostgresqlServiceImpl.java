package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.service.DataBaseInitService;
import com.mntviews.bridge.service.ScriptRunner;
import com.mntviews.bridge.service.exception.DataBaseInitServiceException;
import lombok.RequiredArgsConstructor;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;


@RequiredArgsConstructor
public class DataBaseInitPostgresqlServiceImpl implements DataBaseInitService {

    private final String DDL_CREATE_PATH = "db/postgresql/ddl_create.sql";
    private final String DDL_DROP_PATH = "db/postgresql/ddl_drop.sql";

    private final MetaInitRepo metaInitRepo;

    @Override
    public void migrate(ConnectionData connectionData, Boolean isClean) {
      /*  Flyway flyway = Flyway.configure()
                .dataSource(connectionData.getUrl(), connectionData.getUserName(), connectionData.getPassword()).locations("classpath:db/migration/postgresql")
                .schemas(BridgeContext.DEFAULT_SCHEMA_NAME)
                .load();
        if (isClean)
            flyway.clean();
        flyway.migrate();
*/

        ScriptRunner scriptRunner = new ScriptRunner(metaInitRepo.getConnection(connectionData), false, false);

        if (isClean) {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(DDL_DROP_PATH);
            if (inputStream == null)
                throw new DataBaseInitServiceException(DDL_DROP_PATH + " not found.");
            try {
                scriptRunner.runScript(new InputStreamReader(inputStream), connectionData.getSchemaName());
            } catch (Exception e) {
                throw new DataBaseInitServiceException(e);
            }
        }

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(DDL_CREATE_PATH);
        if (inputStream == null)
            throw new DataBaseInitServiceException(DDL_CREATE_PATH + " not found.");
        try {
            scriptRunner.runScript(new InputStreamReader(inputStream), connectionData.getSchemaName());
        } catch (Exception e) {
            throw new DataBaseInitServiceException(e);
        }

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
    public void clear(ConnectionData connectionData, String groupTag,String metaTag) {
        metaInitRepo.clear(metaInitRepo.getConnection(connectionData), groupTag, metaTag,  connectionData.getSchemaName());
    }


    @Override
    public Connection getConnection(ConnectionData connectionData) {
        return metaInitRepo.getConnection(connectionData);
    }
}
