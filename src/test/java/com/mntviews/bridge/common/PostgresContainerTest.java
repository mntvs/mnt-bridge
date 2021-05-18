package com.mntviews.bridge.common;

import com.mntviews.bridge.model.ConnectionData;
import com.mntviews.bridge.service.BridgeContext;
import com.mntviews.bridge.service.FlyWayService;
import com.mntviews.bridge.service.impl.FlyWayServicePostgresqlImpl;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Objects;
import java.util.Properties;

@Testcontainers
public class PostgresContainerTest {
    public final static String GROUP_TAG = "TEST_GROUP";
    public final static String META_TAG = "TEST_META";
    public final static String SCHEMA_NAME = "TEST_SCHEMA";

    public final static String DB_NAME = "mnt";
    public final static String USER_NAME = BridgeContext.DEFAULT_SCHEMA_NAME;
    public final static String USER_PASSWORD = "mnt";

    public final static Integer ITEMS_COUNT = 10;

    public final static Integer TASK_COUNT = 30;

    protected JdbcTemplate jdbcTemplate;

    protected Connection connection;

    protected ConnectionData connectionData;

    @Container
    protected PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer("postgres:12")
            .withDatabaseName(DB_NAME)
            .withUsername(USER_NAME)
            .withPassword(USER_PASSWORD);

    /**
     * Initialization for postgres test container. Gets sql scripts from db.migration anp process them to the database
     */
    @BeforeEach
    protected void init() throws SQLException {

        FlyWayService flyWayService = new FlyWayServicePostgresqlImpl();
        flyWayService.migrate(new ConnectionData(postgresqlContainer.getJdbcUrl(), postgresqlContainer.getUsername(), postgresqlContainer.getPassword()));

        String url = "jdbc:postgresql://localhost/test";
        Properties props = new Properties();
        props.setProperty("user", postgresqlContainer.getUsername());
        props.setProperty("password", postgresqlContainer.getPassword());
        props.setProperty("escapeSyntaxCallMode", "callIfNoReturn");
        connection = DriverManager.getConnection(postgresqlContainer.getJdbcUrl(), props);
        connection.setAutoCommit(false);

        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgresqlContainer.getJdbcUrl());
        dataSource.setUser(postgresqlContainer.getUsername());
        dataSource.setPassword(postgresqlContainer.getPassword());
        dataSource.setProperty("escapeSyntaxCallMode", "callIfNoReturn");
//dataSource.setDefaultAutoCommit(false);
        connectionData = new ConnectionData(postgresqlContainer.getJdbcUrl(), USER_NAME, USER_PASSWORD);

        jdbcTemplate = new JdbcTemplate((DataSource) dataSource);

        jdbcTemplate.execute("create schema " + SCHEMA_NAME);

        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(c -> {
            PreparedStatement ps = c.prepareStatement(String.format("insert into %s.bridge_group (tag,schema_name) values (?,?) returning id", BridgeContext.DEFAULT_SCHEMA_NAME), Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, GROUP_TAG);
            ps.setString(2, SCHEMA_NAME);
            return ps;
        }, holder);

        jdbcTemplate.update(String.format("insert into %s.bridge_meta (tag,group_id) values (?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), META_TAG, Objects.requireNonNull(holder.getKey()).longValue());

        jdbcTemplate.update(String.format("call %s.prc_create_meta_by_tag(?,?)", BridgeContext.DEFAULT_SCHEMA_NAME), GROUP_TAG, META_TAG);

    }

}
