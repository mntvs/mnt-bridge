package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.exception.BridgeServiceException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MetaInitPostgresqlRepoImpl extends MetaInit {

    @Override
    public Connection getConnection(ConnectionData connectionData) {
        Properties props = new Properties();
        props.setProperty("user", connectionData.getUserName());
        props.setProperty("password", connectionData.getPassword());
        props.setProperty("escapeSyntaxCallMode", "callIfNoReturn");
        try {
            Connection connection = DriverManager.getConnection(connectionData.getUrl(), props);
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            throw new BridgeServiceException(e);
        }
    }
}
