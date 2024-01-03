package com.singlestore.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class IntegrationTestBase {
    static String host = "127.0.0.1";
    static String port = "3306";
    static String user = "root";
    static String password = System.getenv("ROOT_PASSWORD");
    static String database = "db";

    static SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(ImmutableMap.of(
        "host", host,
        "port", port,
        "user", user,
        "password", password
    ));

    @BeforeAll
    static void init() throws SQLException {
        try (
            Connection conn = JDBCUtil.createConnection(conf);
            Statement stmt = conn.createStatement()
            ) {
            stmt.executeQuery(String.format("DROP DATABASE IF EXISTS `%s`", database));
            stmt.executeQuery(String.format("CREATE DATABASE `%s`", database));
        }
    }

    @Test
    // This is a small test to check that GitHub actions work well
    public void test() throws SQLException {
        try (
            Connection conn = JDBCUtil.createConnection(conf);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1")
            ) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }
}
