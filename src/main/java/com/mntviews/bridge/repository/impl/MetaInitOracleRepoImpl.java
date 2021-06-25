package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.repository.exception.MetaInitException;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.exception.BridgeServiceException;

import java.sql.*;
import java.util.Properties;

public class MetaInitOracleRepoImpl implements MetaInitRepo {
    @Override
    public void init(Connection connection, String groupTag, String metaTag, String schemaName, String schemaMetaName) {

        try {
    /*        if (isClean) {
                try (CallableStatement callableStatement = connection.prepareCall(String.format("call %s.prc_drop_meta_by_tag(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME))) {
                    callableStatement.setString(1, groupTag);
                    callableStatement.setString(2, metaTag);
                    callableStatement.executeUpdate();
                }
            }
*/
            try (CallableStatement callableStatement = connection.prepareCall(String.format("call %s.prc_create_meta_by_tag(?,?,?)", BridgeContext.DEFAULT_SCHEMA_NAME))) {
                callableStatement.setString(1, groupTag);
                callableStatement.setString(2, metaTag);
                callableStatement.setString(3, schemaName);
                callableStatement.executeUpdate();
            }
            connection.commit();
        } catch (SQLException e) {
            throw new MetaInitException(e);
        }
    }

    @Override
    public void clear(Connection connection, String groupTag, String metaTag, String schemaMetaName) {
        try (CallableStatement callableStatement = connection.prepareCall(String.format("call %s.prc_drop_meta_by_tag(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME))) {
            callableStatement.setString(1, groupTag);
            callableStatement.setString(2, metaTag);
            callableStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            throw new MetaInitException(e);
        }
    }


    @Override
    public Connection getConnection(ConnectionData connectionData) {
        Properties props = new Properties();
        props.setProperty("user", connectionData.getUserName());
        props.setProperty("password", connectionData.getPassword());
        try {
            Connection connection = DriverManager.getConnection(connectionData.getUrl(), props);
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            throw new BridgeServiceException(e);
        }
    }
}
