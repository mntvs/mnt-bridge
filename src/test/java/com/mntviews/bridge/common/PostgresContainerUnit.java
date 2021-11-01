package com.mntviews.bridge.common;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.DataBaseType;
import com.mntviews.bridge.service.ParamEnum;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class PostgresContainerUnit extends ContainerUnit {

    public final static String dbTypeName = "POSTGRES";
    public final static String DB_URL = "jdbc:postgresql://localhost:5432/postgres";

    public final static String USER_NAME = "postgres";
    public final static String USER_PASSWORD = "123";

    public PostgresContainerUnit() {
        connectionData = new ConnectionData(DB_URL, USER_NAME, USER_PASSWORD, BridgeContext.DEFAULT_SCHEMA_NAME);
        attemptTestParam = "{\"ORDER\": \"LIFO\",\"ATTEMPT\": 2}";
        bridgeContext = BridgeContext
                .custom(GROUP_TAG, META_TAG, connectionData)
                .withAfterProcessing((connection, processData) -> {
                    if (processData.getRawId() % 2 == 0)
                        throw new RuntimeException(TEST_EXCEPTION_TEXT);
                })
                .withSchemaName(SCHEMA_NAME)
                .withDataBaseType(DataBaseType.POSTGRESQL)
                .build();

        Map<String, Object> param = new HashMap<>();
        param.put(ParamEnum.ATTEMPT.name(), 2);
        bridgeContextAttempt = BridgeContext
                .custom(GROUP_TAG, META_TAG, connectionData)
                .withAfterProcessing((connection, processData) -> {
                    throw new RuntimeException(TEST_EXCEPTION_TEXT);
                })
                .withParam(param)
                .withSchemaName(SCHEMA_NAME)
                .withDataBaseType(DataBaseType.POSTGRESQL)
                .build();

        connection = bridgeContext.findConnection();

        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(DB_URL);
        dataSource.setUser(USER_NAME);
        dataSource.setPassword(USER_PASSWORD);
        try {
            dataSource.setProperty("escapeSyntaxCallMode", "callIfNoReturn");


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        jdbcTemplate = new JdbcTemplate(dataSource);

    }

    @Override
    public String wrapCodeBlock(String codeBlock) {
        return "DO $$" + codeBlock + "$$";
    }

    @Override
    public String findTestProcedure(String typeTag) {
        switch (typeTag) {
            case "EXCEPTION":
                return "begin EXECUTE format(\n" +
                        "                $string$ create or replace procedure %s(a_raw_id bigint, a_buf_id bigint)\n" +
                        "                   language plpgsql\n" +
                        "                   as\n" +
                        "$f1$\n" +
                        "begin\n" +
                        "    raise exception 'Test exception'; \n" +
                        "exception when others then\n" +
                        "    raise exception '%s error : %% {buf.id=%%}', sqlerrm, a_buf_id;\n" +
                        "end;\n" +
                        "$f1$;\n" +
                        "                   $string$, '" + bridgeContext.getMetaData().getPrcExecFullName() + "', lower('" + bridgeContext.getMetaData().getPrcExecName() + "')); end;";

            case "EXCEPTION2":
                return "begin EXECUTE format(\n" +
                        "                $string$ create or replace procedure %s(a_raw_id bigint, a_buf_id bigint)\n" +
                        "                   language plpgsql\n" +
                        "                   as\n" +
                        "$f1$\n" +
                        "begin\n" +
                        "    if a_raw_id%%2=0 then raise exception '" + TEST_EXCEPTION_TEXT + "'; end if; \n" +
                        "exception when others then\n" +
                        "    raise exception '%s error : %% {buf.id=%%}', sqlerrm, a_buf_id; \n" +
                        "end;\n" +
                        "$f1$;\n" +
                        "                   $string$, '" + bridgeContext.getMetaData().getPrcExecFullName() + "', lower('" + bridgeContext.getMetaData().getPrcExecName() + "')); end;";

            case "EXCEPTION3":
                return "begin EXECUTE format(\n" +
                        "                $string$ create or replace procedure %s(a_raw_id bigint, a_buf_id bigint)\n" +
                        "                   language plpgsql\n" +
                        "                   as\n" +
                        "$f1$\n" +
                        "begin\n" +
                        "    raise exception '" + TEST_EXCEPTION_TEXT + "' USING ERRCODE = '20993'; \n" +
                        "exception when others then\n" +
                        "    raise exception '%s error : %% {buf.id=%%}', sqlerrm, a_buf_id USING ERRCODE=sqlstate; \n" +
                        "end;\n" +
                        "$f1$;\n" +
                        "                   $string$, '" + bridgeContext.getMetaData().getPrcExecFullName() + "', lower('" + bridgeContext.getMetaData().getPrcExecName() + "')); end;";

            default:
                throw new RuntimeException("typeTag not found {" + typeTag + "}");
        }
    }

    @Override
    public String findDbTypeName() {
        return dbTypeName;
    }
}
