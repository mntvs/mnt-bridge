package com.mntviews.bridge.common;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.DataBaseType;
import com.mntviews.bridge.service.ParamEnum;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class OracleContainerUnit extends ContainerUnit {

    public final static String dbTypeName = "ORACLE";
    public final static String DB_URL = "jdbc:oracle:thin:@localhost:1521:xe";

    public final static String USER_NAME = "mnt_bridge";
    public final static String USER_PASSWORD = "mnt_bridge";

    public OracleContainerUnit() {

        connectionData = new ConnectionData(DB_URL, USER_NAME, USER_PASSWORD, BridgeContext.DEFAULT_SCHEMA_NAME);
        attemptTestParam = "<PARAM><ORDER>LIFO</ORDER><ATTEMPT>2</ATTEMPT></PARAM>";
        skipTestParam = "<PARAM><ORDER>LIFO</ORDER><ATTEMPT>-1</ATTEMPT><SKIP>1</SKIP></PARAM>";
        bridgeContext = BridgeContext
                .custom(GROUP_TAG, META_TAG, connectionData)
                .withAfterProcessing((connection, processData, context) -> {
                    if (processData.getRawId() % 2 == 0)
                        throw new RuntimeException(TEST_EXCEPTION_TEXT);
                })
                .withSchemaName(SCHEMA_NAME)
                .withDataBaseType(DataBaseType.ORACLE)
                .build();

        Map<String, Object> param = new HashMap<>();
        param.put(ParamEnum.ATTEMPT.name(), 2);
        bridgeContextAttempt = BridgeContext
                .custom(GROUP_TAG, META_TAG, connectionData)
                .withAfterProcessing((connection, processData, context) -> {
                    throw new RuntimeException(TEST_EXCEPTION_TEXT);
                })
                .withParam(param)
                .withSchemaName(SCHEMA_NAME)
                .withDataBaseType(DataBaseType.ORACLE)
                .build();

        param = new HashMap<>();
        param.put(ParamEnum.SKIP.name(), 1);
        bridgeContextSkip = BridgeContext
                .custom(GROUP_TAG, META_TAG, connectionData)
                .withAfterProcessing((connection, processData, context) -> {
                    throw new RuntimeException(TEST_EXCEPTION_TEXT);
                })
                .withParam(param)
                .withSchemaName(SCHEMA_NAME)
                .withDataBaseType(DataBaseType.ORACLE)
                .build();

        connection = bridgeContext.findConnection();

        OracleConnectionPoolDataSource dataSource = null;
        try {
            dataSource = new OracleConnectionPoolDataSource();
            dataSource.setURL(DB_URL);
            dataSource.setUser(USER_NAME);
            dataSource.setPassword(USER_PASSWORD);
            Properties props = new Properties();
            //props.put("oracle.jdbc.autoCommitSpecCompliant", "false");
            dataSource.setConnectionProperties(props);
            jdbcTemplate = new JdbcTemplate(dataSource);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public String findTestProcedure(String typeTag) {
        switch (typeTag) {
            case "EXCEPTION":
                return "begin EXECUTE IMMEDIATE\n" +
                        "                'create or replace procedure " + bridgeContext.getMetaData().getPrcExecFullName() + "(a_raw_id number, a_buf_id number)\n" +
                        "                as\n" +
                        "                    l_sqlerrm VARCHAR2(2000);\n" +
                        "                begin\n" +
                        "                raise_application_error(-20001,''Test exception'');\n" +
                        "        exception when others then\n" +
                        "                l_sqlerrm:=sqlerrm;\n" +
                        "            raise_application_error(-20001, ''' || lower('" + bridgeContext.getMetaData().getPrcExecName() + "') || ' error : '' || l_sqlerrm || '' {buf.id='' || a_buf_id || ''}'');\n" +
                        "    end;'; end;";
            case "EXCEPTION2":
                return "begin EXECUTE IMMEDIATE\n" +
                        "                'create or replace procedure " + bridgeContext.getMetaData().getPrcExecFullName() + "(a_raw_id number, a_buf_id number)\n" +
                        "                as\n" +
                        "                    l_sqlerrm VARCHAR2(2000);\n" +
                        "                begin\n" +
                        "               if MOD(a_raw_id,2)=0 then raise_application_error(-20001,''" + TEST_EXCEPTION_TEXT + "''); end if; \n" +
                        "        exception when others then\n" +
                        "                l_sqlerrm:=sqlerrm;\n" +
                        "            raise_application_error(-20001, ''' || lower('" + bridgeContext.getMetaData().getPrcExecName() + "') || ' error : '' || l_sqlerrm || '' {buf.id='' || a_buf_id || ''}'');\n" +
                        "    end;'; end;";

            case "EXCEPTION3":
                return "begin EXECUTE IMMEDIATE\n" +
                        "                'create or replace procedure " + bridgeContext.getMetaData().getPrcExecFullName() + "(a_raw_id number, a_buf_id number)\n" +
                        "                as\n" +
                        "                    l_sqlerrm VARCHAR2(2000);\n" +
                        "                begin\n" +
                        "               raise_application_error(-20993,''" + TEST_EXCEPTION_TEXT + "'');  \n" +
                        "        exception when others then\n" +
                        "                l_sqlerrm:=sqlerrm;\n" +
                        "            raise_application_error(SQLCODE, ''' || lower('" + bridgeContext.getMetaData().getPrcExecName() + "') || ' error : '' || l_sqlerrm || '' {buf.id='' || a_buf_id || ''}'');\n" +
                        "    end;'; end;";
            default:
                throw new RuntimeException("typeTag not found {" + typeTag + "}");
        }
    }

    @Override
    public String findDbTypeName() {
        return dbTypeName;
    }
}