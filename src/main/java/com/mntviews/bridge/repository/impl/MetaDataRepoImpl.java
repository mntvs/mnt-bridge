package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.ProcessData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.exception.MetaDataRepoException;
import com.mntviews.bridge.repository.exception.RawLoopRepoException;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

@RequiredArgsConstructor
public class MetaDataRepoImpl implements MetaDataRepo {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public MetaData findMetaData(Connection connection, String groupTag, String metaTag) {

        try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM bridge.fnc_get_meta_data(?,?)")) {
            stmt.setString(1, groupTag);
            stmt.setString(2, metaTag);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                MetaData metaData = new MetaData();
                metaData.setMetaId(rs.getLong("meta_id"));
                metaData.setGroupId(rs.getLong("group_id"));
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
            throw new MetaDataRepoException(e);
        }

         /*   List<MetaData> metaDataList = jdbcTemplate.query("SELECT * FROM bridge.fnc_get_meta_data(?,?)",
                    (rs, rowNum) -> {
                        MetaData metaData = new MetaData();
                        metaData.setMetaId(rs.getLong("meta_id"));
                        metaData.setGroupId(rs.getLong("group_id"));
                        metaData.setBufFullName(rs.getString("buf_full_name"));
                        metaData.setRawFullName(rs.getString("raw_full_name"));
                        metaData.setRawName(rs.getString("raw_name"));
                        metaData.setBufName(rs.getString("buf_name"));
                        metaData.setSchemaName(rs.getString("schema_name"));
                        metaData.setPrcExecName(rs.getString("prc_exec_name"));
                        metaData.setPrcExecFullName(rs.getString("prc_exec_full_name"));
                        metaData.setRawLoopQuery(rs.getString("raw_loop_query"));
                        return metaData;
                    }, groupTag, metaTag
            );
            if (metaDataList.size() == 0)
                return null;
            else
                return metaDataList.get(0);
*/
        return null;
    }
}
