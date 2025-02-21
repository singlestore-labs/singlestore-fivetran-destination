package com.singlestore.fivetran.destination.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class JDBCUtilTest extends IntegrationTestBase {
    @Test
    public void driverParameters() throws Exception {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(ImmutableMap.of("host", host,
                "port", port, "user", user, "password", password, "driver.parameters",
                "cachePrepStmts = TRUE; allowMultiQueries=  TRUE ;connectTimeout = 20000"));
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement();) {
            stmt.executeQuery("SELECT 1; SELECT 2");
        }
    }
}
