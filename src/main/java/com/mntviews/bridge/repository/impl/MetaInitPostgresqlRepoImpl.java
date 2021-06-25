package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.repository.exception.MetaInitException;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.exception.BridgeServiceException;

import java.sql.*;
import java.util.Properties;

public class MetaInitPostgresqlRepoImpl implements MetaInitRepo {
    @Override
    public void init(Connection connection, String groupTag, String metaTag, String schemaName, String schemaMetaName) {

        try (Statement statement = connection.createStatement()) {
            statement.execute("create schema if not exists " + schemaName);

            Long groupId = null;
            ResultSet resultSet = statement.executeQuery(String.format("select id from %s.bridge_group where tag='%s'", schemaMetaName, groupTag));
            while (resultSet.next()) {
                groupId = resultSet.getLong("id");
            }

            if (groupId == null) {
                statement.execute(String.format("insert into %s.bridge_group (tag,schema_name) values ('%s','%s') returning id", schemaMetaName, groupTag, schemaName), Statement.RETURN_GENERATED_KEYS);
                try (ResultSet keys = statement.getGeneratedKeys()) {
                    if (keys.next())
                        groupId = keys.getLong(1);
                }
            }


            Long metaId = null;
            resultSet = statement.executeQuery(String.format("select id from %s.bridge_meta where tag='%s'", schemaMetaName, metaTag));
            while (resultSet.next()) {
                metaId = resultSet.getLong("id");
            }

            if (metaId == null) {
                statement.execute(String.format("insert into %s.bridge_meta (tag, group_id) values ('%s',%d)", schemaMetaName, metaTag, groupId));

            }

            try (CallableStatement callableStatement = connection.prepareCall(String.format("call %s.prc_create_meta_by_tag(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME))) {
                callableStatement.setString(1, groupTag);
                callableStatement.setString(2, metaTag);
                callableStatement.executeUpdate();
            }

            connection.commit();
        } catch (SQLException e) {
            throw new MetaInitException(e);
        }
    }

    @Override
    public void clear(Connection connection, String groupTag, String metaTag, String schemaMetaName) {

    }


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
