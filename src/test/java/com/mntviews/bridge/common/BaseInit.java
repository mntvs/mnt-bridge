package com.mntviews.bridge.common;

import lombok.extern.java.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;

@Log
public class BaseInit {
    protected List<ContainerUnit> containerUnitList;

    @BeforeEach
    public void init() {

        containerUnitList = new ArrayList<>();
        log.info("OracleContainerUnit init");
        OracleContainerUnit oracleContainerUnit = new OracleContainerUnit();
        oracleContainerUnit.getBridgeContext().migrate(true);
        oracleContainerUnit.getBridgeContext().clear();
        oracleContainerUnit.getBridgeContext().init();

        containerUnitList.add(oracleContainerUnit);

        log.info("PostgresContainerUnit init");
        PostgresContainerUnit postgresContainerUnit = new PostgresContainerUnit();
        postgresContainerUnit.getBridgeContext().migrate(true);
        postgresContainerUnit.getBridgeContext().clear();
        postgresContainerUnit.getBridgeContext().init();
        containerUnitList.add(postgresContainerUnit);

    }

    @AfterEach
    public void clear() {

        containerUnitList.forEach(containerUnit -> containerUnit.getBridgeContext().clear());
    }


}
