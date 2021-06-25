package com.mntviews.bridge.db;

import com.mntviews.bridge.common.OracleContainerUnit;
import com.mntviews.bridge.common.PostgresContainerUnit;
import org.junit.jupiter.api.Test;

public class MetaDataOracleTest  {
    OracleContainerUnit oracleContainerUnit = new OracleContainerUnit();

    PostgresContainerUnit postgresContainerUnit = new PostgresContainerUnit();

    @Test
    void migrateOracleTest() {
        oracleContainerUnit.getBridgeContext().migrate(true);

      //  oracleContainerUnit.getBridgeContext().init();
    }

    @Test
    void migratePostgresTest() {
        postgresContainerUnit.getBridgeContext().migrate(true);

        //  oracleContainerUnit.getBridgeContext().init();
    }
}
