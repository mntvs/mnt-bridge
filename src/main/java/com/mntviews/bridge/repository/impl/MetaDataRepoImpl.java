package com.mntviews.bridge.repository.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.repository.MetaDataRepo;
import com.mntviews.bridge.repository.exception.MetaDataRepoException;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.ParamTypeEnum;
import lombok.RequiredArgsConstructor;

import javax.xml.bind.annotation.XmlType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

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
                    metaData.setParamType(rs.getString("param_type"));
                    String param =rs.getString("param");
                    if (param == null)
                        throw new MetaDataRepoException("MetaData.param must be not null");

                    metaData.setParam(ParamTypeEnum.valueOf(metaData.getParamType()).toValue(rs.getString("param")));
                    return metaData;
                }

            } catch (Exception e) {
                throw new MetaDataRepoException(e);
            }
        } catch (Exception e) {
            throw new MetaDataRepoException(e);
        }

        return null;
    }
}
