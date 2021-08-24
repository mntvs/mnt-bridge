package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.exception.MetaDataRepoException;
import com.mntviews.bridge.service.BridgeContext;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@RequiredArgsConstructor
public class MetaDataRepoImpl implements MetaDataRepo {

    @Override
    public MetaData findMetaData(Connection connection, String groupTag, String metaTag, String schemaName) {
        try {
            try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + schemaName + ".bridge_meta_v where group_tag=? and meta_tag=?")) {
                stmt.setString(1, groupTag);
                stmt.setString(2, metaTag);
                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    MetaData metaData = new MetaData();
                    metaData.setMetaId(rs.getBigDecimal("meta_id"));
                    metaData.setGroupId(rs.getBigDecimal("group_id"));
                    metaData.setBufFullName(rs.getString("buf_full_name"));
                    metaData.setRawFullName(rs.getString("raw_full_name"));
                    metaData.setRawName(rs.getString("raw_name"));
                    metaData.setBufName(rs.getString("buf_name"));
                    metaData.setSchemaName(rs.getString("schema_name"));
                    metaData.setPrcExecName(rs.getString("prc_exec_name"));
                    metaData.setPrcExecFullName(rs.getString("prc_exec_full_name"));
                    metaData.setRawLoopQuery(rs.getString("raw_loop_query"));

                    return metaData;
                }

            } catch (Exception e) {
                connection.rollback();
                throw new MetaDataRepoException(e);
            } finally {
                connection.commit();
            }
        } catch (Exception e) {
            throw new MetaDataRepoException(e);
        }

        return null;
    }
}
