package com.mntviews.bridge.repository.impl;

import com.mntviews.bridge.model.MetaData;
import com.mntviews.bridge.model.RawData;
import com.mntviews.bridge.repository.RawRepo;
import com.mntviews.bridge.repository.exception.MetaDataRepoException;
import com.mntviews.bridge.repository.exception.RawRepoException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.Objects;

public class RawRepoImpl implements RawRepo {

    @Override
    public RawData saveRawData(Connection connection, String rawFullName, RawData rawData) {
        try {
            if (rawData.getId() == null) {
                try (PreparedStatement stmt = connection
                        .prepareStatement("INSERT INTO " + rawFullName + "(f_id,f_date,f_oper,f_msg,f_payload) values (?,?,?,?,?)", new String[]{"id"})) {
                    stmt.setString(1, rawData.getFId());
                    stmt.setObject(2, Objects.requireNonNullElse(rawData.getFDate(), OffsetDateTime.now()));
                    stmt.setByte(3, Objects.requireNonNullElse(rawData.getFOper(), 0).byteValue());
                    stmt.setString(4, rawData.getFMsg());
                    stmt.setString(5, rawData.getFPayload());

                    int count = stmt.executeUpdate();

                    if (count == 0) {
                        throw new RawRepoException("0 rows affected");
                    }

                    try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                        if (generatedKeys.next()) {
                            rawData.setId(generatedKeys.getBigDecimal(1));
                        } else {
                            throw new RawRepoException("Insert raw error");
                        }
                    }

                } catch (Exception e) {
                    connection.rollback();
                    throw new RawRepoException(e);
                } finally {
                    connection.commit();
                }
            } else {
                try (PreparedStatement stmt = connection
                        .prepareStatement("UPDATE " + rawFullName + " SET s_action=? WHERE id=?")) {
                    stmt.setByte(1, rawData.getSAction());
                    stmt.setBigDecimal(2, rawData.getId());
                    int count = stmt.executeUpdate();

                    if (count == 0) {
                        throw new RawRepoException("0 rows affected");
                    }
                } catch (Exception e) {
                    connection.rollback();
                    throw new RawRepoException(e);
                } finally {
                    connection.commit();
                }
            }

        } catch (Exception e) {
            throw new RawRepoException(e);
        }
        return rawData;
    }

    @Override
    public RawData findRawDataById(Connection connection, String rawFullName, Long id) {
        try {
            try (PreparedStatement stmt = connection
                    .prepareStatement("SELECT id,f_id,f_date,s_date,f_oper,f_msg,s_msg,s_status,s_action,f_payload,s_counter FROM " + rawFullName + " where id=?")) {
                stmt.setLong(1, id);
                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    RawData rawData = new RawData();
                    rawData.setId(rs.getBigDecimal("id"));
                    rawData.setFId(rs.getString("f_id"));
                    rawData.setFDate(rs.getObject("f_date", OffsetDateTime.class));
                    rawData.setSDate(rs.getObject("s_date", OffsetDateTime.class));
                    rawData.setFOper(rs.getByte("f_oper"));
                    rawData.setFMsg(rs.getString("f_msg"));
                    rawData.setSMsg(rs.getString("s_msg"));
                    rawData.setSStatus(rs.getByte("s_status"));
                    rawData.setSAction(rs.getByte("s_action"));
                    rawData.setFPayload(rs.getString("f_payload"));
                    rawData.setSCounter(rs.getInt("s_counter"));
                    return rawData;
                }

            } catch (Exception e) {
                connection.rollback();
                throw new RawRepoException(e);
            } finally {
                connection.commit();
            }
        } catch (Exception e) {
            throw new RawRepoException(e);
        }
        return null;
    }
}
