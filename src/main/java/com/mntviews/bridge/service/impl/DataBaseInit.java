package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.service.BridgeUtil;
import com.mntviews.bridge.service.DataBaseInitService;
import com.mntviews.bridge.service.ScriptRunner;
import com.mntviews.bridge.service.exception.DataBaseInitServiceException;
import lombok.RequiredArgsConstructor;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;


@RequiredArgsConstructor
abstract public class DataBaseInit implements DataBaseInitService {
    protected final MetaInitRepo metaInitRepo;
    protected final MetaDataRepo metaDataRepo;

    protected MetaData init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName, String param) {
        if (metaInitRepo != null) {
            try (Connection connection = metaInitRepo.getConnection(connectionData)) {
                MetaData metaData = metaDataRepo.findMetaData(connection, groupTag, metaTag, connectionData.getSchemaName());
                if (metaData == null) {
                    metaInitRepo.init(connection, groupTag, metaTag, schemaName, connectionData.getSchemaName(), param);
                    metaData = metaDataRepo.findMetaData(connection, groupTag, metaTag, connectionData.getSchemaName());
                }
                try {
                    connection.commit();
                } catch (SQLException e) {
                    throw new DataBaseInitServiceException(e);
                }
                return metaData;
            } catch (SQLException e) {
                throw new DataBaseInitServiceException(e);
            }


        }
        return null;
    }

    protected void migrate(ConnectionData connectionData, Boolean isClean, String ddlCreatePath, String ddlDropPath) {
        try (Connection connection = metaInitRepo.getConnection(connectionData)) {
            ScriptRunner scriptRunner = new ScriptRunner(connection, false, false);
            try {
                if (isClean) {
                    executeScript(connectionData, ddlDropPath, scriptRunner);
                    connection.commit();

                }
                executeScript(connectionData, ddlCreatePath, scriptRunner);
                connection.commit();
            } catch (Exception e) {
                throw new DataBaseInitServiceException(e);
            }
        } catch (SQLException e) {
            throw new DataBaseInitServiceException(e);
        }

    }

    private void executeScript(ConnectionData connectionData, String ddlCreatePath, ScriptRunner scriptRunner) {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(ddlCreatePath);
        if (inputStream == null)
            throw new DataBaseInitServiceException(ddlCreatePath + " not found.");
        try {
            String versionStr = BridgeUtil.BUILD_INFO.getProperty("name") + " ver. " + BridgeUtil.BUILD_INFO.getProperty("version");
            scriptRunner.runScript(new InputStreamReader(inputStream), connectionData.getSchemaName(), versionStr);
        } catch (Exception e) {
            throw new DataBaseInitServiceException(e);
        }
    }

    @Override
    public void migrate(ConnectionData connectionData) {
        migrate(connectionData, false);
    }


    @Override
    public void clear(ConnectionData connectionData, String groupTag, String metaTag) {

        try (Connection connection = metaInitRepo.getConnection(connectionData)) {
            if (metaInitRepo != null) {
                metaInitRepo.clear(connection, groupTag, metaTag, connectionData.getSchemaName());
            }
        } catch (SQLException e) {
            throw new DataBaseInitServiceException(e);
        }

    }


    @Override
    public Connection getConnection(ConnectionData connectionData) {
        if (metaInitRepo != null)
            return metaInitRepo.getConnection(connectionData);
        else
            return null;
    }

}
