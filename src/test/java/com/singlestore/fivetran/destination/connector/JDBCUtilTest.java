package com.singlestore.fivetran.destination.connector;

import java.sql.Connection;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    public void connectionError() {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(ImmutableMap.of("host", "wrongHost",
                "port", "1234", "user", user, "password", "wrongPassword"));

        Exception exception = assertThrows(SQLNonTransientConnectionException.class, () -> {
            JDBCUtil.createConnection(conf);
        });
        assertTrue(exception.getMessage().contains(
                "Failed to connect to SingleStore at wrongHost:1234.\n\n" +
                        "Try these steps to resolve it:\n" +
                        "  1) Verify your SingleStore cluster is running and listening on port 1234.\n" +
                        "  2) Confirm Fivetran’s IPs for your region are allowlisted in your firewall: https://fivetran.com/docs/using-fivetran/ips\n" +
                        "  3) Ensure the hostname and port in the destination configuration are correct (host=wrongHost, port=1234).\n\n" +
                        "Original error: "
        ));
    }

}
