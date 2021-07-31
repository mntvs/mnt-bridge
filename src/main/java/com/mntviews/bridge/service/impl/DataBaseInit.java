package com.mntviews.bridge.service.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.service.DataBaseInitService;
import com.mntviews.bridge.service.ScriptRunner;
import com.mntviews.bridge.service.exception.DataBaseInitServiceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;


@RequiredArgsConstructor
abstract public class DataBaseInit implements DataBaseInitService {
    protected final MetaInitRepo metaInitRepo;
    protected final MetaDataRepo metaDataRepo;

    public MetaData init(ConnectionData connectionData, String groupTag, String metaTag, String schemaName) {
        if (metaInitRepo != null) {
            Connection connection = metaInitRepo.findConnection(connectionData);
            MetaData metaData = metaDataRepo.findMetaData(connection, groupTag, metaTag, connectionData.getSchemaName());
            if (metaData == null) {
                metaInitRepo.init(connection, groupTag, metaTag, schemaName, connectionData.getSchemaName());
                metaData = metaDataRepo.findMetaData(connection, groupTag, metaTag, connectionData.getSchemaName());
            }
            return metaData;
        }
        return null;
    }

    protected void migrate(ConnectionData connectionData, Boolean isClean, String ddlCreatePath, String ddlDropPath) {
        Connection connection = metaInitRepo.findConnection(connectionData);
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
    }

    private void executeScript(ConnectionData connectionData, String ddlCreatePath, ScriptRunner scriptRunner) {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(ddlCreatePath);
        if (inputStream == null)
            throw new DataBaseInitServiceException(ddlCreatePath + " not found.");
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
    public void clear(ConnectionData connectionData, String groupTag, String metaTag) {
        if (metaInitRepo != null)
            metaInitRepo.clear(metaInitRepo.findConnection(connectionData), groupTag, metaTag, connectionData.getSchemaName());
    }


    @Override
    public Connection getConnection(ConnectionData connectionData) {
        if (metaInitRepo != null)
            return metaInitRepo.findConnection(connectionData);
        else
            return null;
    }

}
