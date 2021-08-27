package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.BufData;
import com.mntviews.bridge.repository.BufRepo;
import com.mntviews.bridge.repository.exception.RawRepoException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;

public class BufRepoImpl implements BufRepo {
    @Override
    public BufData findBufDataById(Connection connection, String bufFullName, Long id) {
        return findBufDataBy(connection, bufFullName, id, "id");
    }

    private BufData findBufDataBy(Connection connection, String bufFullName, Long id, String idName) {
        try {
            try (PreparedStatement stmt = connection
                    .prepareStatement("SELECT id,f_id,f_date,s_date,f_oper,f_payload,s_payload,s_counter,f_raw_id FROM " + bufFullName + " where " + idName + "=?")) {
                stmt.setLong(1, id);
                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    BufData bufData = new BufData();
                    bufData.setId(rs.getLong("id"));
                    bufData.setFId(rs.getString("f_id"));
                    bufData.setFDate(rs.getObject("f_date", OffsetDateTime.class));
                    bufData.setSDate(rs.getObject("s_date", OffsetDateTime.class));
                    bufData.setFOper(rs.getByte("f_oper"));
                    bufData.setFPayload(rs.getString("f_payload"));
                    bufData.setSPayload(rs.getString("s_payload"));
                    bufData.setSCounter(rs.getInt("s_counter"));
                    bufData.setFRawId(rs.getLong("f_raw_id"));
                    return bufData;
                }

            } catch (Exception e) {
                throw new RawRepoException(e);
            }
        } catch (Exception e) {
            throw new RawRepoException(e);
        }
        return null;
    }

    @Override
    public BufData findBufDataByRawId(Connection connection, String bufFullName, Long id) {
        return findBufDataBy(connection, bufFullName, id, "f_raw_id");
    }
}
