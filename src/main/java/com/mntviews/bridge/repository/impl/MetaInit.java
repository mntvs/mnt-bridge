package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.repository.MetaInitRepo;
import com.mntviews.bridge.repository.exception.MetaInitException;
import com.mntviews.bridge.service.BridgeContext;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

abstract public class MetaInit implements MetaInitRepo {

    @Override
    public void init(Connection connection, String groupTag, String metaTag, String schemaName, String schemaMetaName) {

        try {
            try (CallableStatement callableStatement = connection.prepareCall(String.format("call %s.prc_create_meta_by_tag(?,?,?)", schemaMetaName))) {
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
        try (CallableStatement callableStatement = connection.prepareCall(String.format("call %s.prc_drop_meta_by_tag(?,?)", schemaMetaName))) {
            callableStatement.setString(1, groupTag);
            callableStatement.setString(2, metaTag);
            callableStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            throw new MetaInitException(e);
        }
    }

}
