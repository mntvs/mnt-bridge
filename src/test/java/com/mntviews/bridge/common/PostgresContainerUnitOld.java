package com.mntviews.bridge.common;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.DataBaseType;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;

@Testcontainers
public class PostgresContainerUnitOld {
    public final static String GROUP_TAG = "TEST_GROUP";
    public final static String META_TAG = "TEST_META";
    public final static String SCHEMA_NAME = "TEST_SCHEMA";

    final static String ERROR_LOG_TEXT = "Error";

    public final static String DB_NAME = "mnt";
    public final static String USER_NAME = BridgeContext.DEFAULT_SCHEMA_NAME;
    public final static String USER_PASSWORD = "mnt";

    public final static Integer ITEMS_COUNT = 100;

    public final static Integer TASK_COUNT = 50;

    protected JdbcTemplate jdbcTemplate;

    protected Connection connection;

    protected ConnectionData connectionData;

    @Container
    protected PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer("postgres:12")
            .withDatabaseName(DB_NAME)
            .withUsername(USER_NAME)
            .withPassword(USER_PASSWORD);

    protected BridgeContext bridgeContext;

    /**
     * Initialization for postgres test container. Gets sql scripts from db.migration anp process them to the database
     */
    @BeforeEach
    protected void init() throws SQLException {

        //DataBaseInitService dataBaseInitService = new DataBaseInitPostgresqlServiceImpl(new MetaInitPostgresqlRepoImpl());
        //dataBaseInitService.migrate(new ConnectionData(postgresqlContainer.getJdbcUrl(), postgresqlContainer.getUsername(), postgresqlContainer.getPassword(), BridgeContext.DEFAULT_SCHEMA_NAME));

        //String url = "jdbc:postgresql://localhost/test";
        connectionData = new ConnectionData(postgresqlContainer.getJdbcUrl(), USER_NAME, USER_PASSWORD, BridgeContext.DEFAULT_SCHEMA_NAME);


        bridgeContext = BridgeContext
                .custom(GROUP_TAG, META_TAG, connectionData)
                .withBridgeProcessing((connection, processData) -> {
                    if (processData.getRawId() < 0)
                        throw new RuntimeException(ERROR_LOG_TEXT);
                })
                .withSchemaName(SCHEMA_NAME)
                .withDataBaseType(DataBaseType.POSTGRESQL)
                .build();

        bridgeContext.migrate();

        connection = bridgeContext.getConnection();

        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgresqlContainer.getJdbcUrl());
        dataSource.setUser(postgresqlContainer.getUsername());
        dataSource.setPassword(postgresqlContainer.getPassword());
        dataSource.setProperty("escapeSyntaxCallMode", "callIfNoReturn");

        jdbcTemplate = new JdbcTemplate(dataSource);

        bridgeContext.init();
     /*   jdbcTemplate.execute("create schema " + SCHEMA_NAME);

        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(c -> {
            PreparedStatement ps = c.prepareStatement(String.format("insert into %s.bridge_group (tag,schema_name) values (?,?) returning id", BridgeContext.DEFAULT_SCHEMA_NAME), Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, GROUP_TAG);
            ps.setString(2, SCHEMA_NAME);
            return ps;
        }, holder);

        jdbcTemplate.update(String.format("insert into %s.bridge_meta (tag,group_id) values (?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), META_TAG, Objects.requireNonNull(holder.getKey()).longValue());

        jdbcTemplate.update(String.format("call %s.prc_create_meta_by_tag(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);
*/
    }

}