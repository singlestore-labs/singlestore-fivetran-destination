package com.singlestore.fivetran.destination.connector;

import com.singlestore.fivetran.destination.connector.writers.UpdateHistoryWriter;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UpdateHistoryWriterTest extends IntegrationTestBase {
    @Test
    public void noFivetranStart() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE noFivetranStartUpdate(" +
                    "id INT PRIMARY KEY, " +
                    "data TEXT, " +
                    "_fivetran_active BOOL, " +
                    "_fivetran_end DATETIME(6))");
            stmt.execute("INSERT INTO noFivetranStartUpdate VALUES(1, 'a', TRUE, '2005-05-24 20:57:00.000000')");

            Table t = JDBCUtil.getTable(conf, database, "noFivetranStartUpdate", "noFivetranStartUpdate", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            Exception exception = assertThrows(IllegalArgumentException.class, () -> {
                u.setHeader(Arrays.asList("id", "data", "_fivetran_active", "_fivetran_end"));
            });

            assertEquals("File doesn't contain _fivetran_start column", exception.getMessage());
        }
    }

    @Test
    public void singlePK() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE singlePKUpdate(" +
                    "id INT, " +
                    "data TEXT, " +
                    "_fivetran_active BOOL, " +
                    "_fivetran_start DATETIME(6)," +
                    "_fivetran_end DATETIME(6)," +
                    "PRIMARY KEY(id, _fivetran_start))");
            stmt.execute("INSERT INTO singlePKUpdate VALUES(1, 'a', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO singlePKUpdate VALUES(2, 'b', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO singlePKUpdate VALUES(3, 'c', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO singlePKUpdate VALUES(1, 'd', FALSE, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO singlePKUpdate VALUES(2, 'e', FALSE, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");

            Table t = JDBCUtil.getTable(conf, database, "singlePKUpdate", "singlePKUpdate", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            u.setHeader(Arrays.asList("id", "data", "_fivetran_start"));
            u.writeRow(Arrays.asList("1", "f", "2005-05-25T20:57:00Z"));
            u.writeRow(Arrays.asList("2", "unm", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("5", "unm", "2005-05-26T20:57:00Z"));
            u.commit();
        }

        checkResult("SELECT * FROM `singlePKUpdate` ORDER BY id, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "d", "0", "2005-05-23 20:57:00.000000", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "a", "0", "2005-05-24 20:57:00.000000", "2005-05-25 20:56:59.999999"),
                Arrays.asList("1", "f", "1", "2005-05-25 20:57:00.000000", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "e", "0", "2005-05-23 20:57:00.000000", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "b", "0", "2005-05-24 20:57:00.000000", "2005-05-26 20:56:59.999999"),
                Arrays.asList("2", "b", "1", "2005-05-26 20:57:00.000000", "9999-12-31 23:59:59.999999"),
                Arrays.asList("3", "c", "1", "2005-05-24 20:57:00.000000", "9999-12-31 23:59:59.999999")
        ));
    }

    @Test
    public void multiPK() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE multiPKUpdate(" +
                    "id1 INT, " +
                    "id2 INT, " +
                    "data1 TEXT, " +
                    "data2 TEXT, " +
                    "_fivetran_active BOOL, " +
                    "_fivetran_start DATETIME(6)," +
                    "_fivetran_end DATETIME(6)," +
                    "PRIMARY KEY(id1, id2, _fivetran_start))");
            stmt.execute("INSERT INTO multiPKUpdate VALUES(1, 1, 'a', 'a', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO multiPKUpdate VALUES(1, 2, 'a', 'a', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO multiPKUpdate VALUES(2, 2, 'b', 'b', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO multiPKUpdate VALUES(3, 3, 'c', 'c', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO multiPKUpdate VALUES(1, 1, 'd', 'd', FALSE, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO multiPKUpdate VALUES(2, 2, 'e', 'e', FALSE, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");

            Table t = JDBCUtil.getTable(conf, database, "multiPKUpdate", "multiPKUpdate", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            u.setHeader(Arrays.asList("id1", "id2", "data1", "data2", "_fivetran_start"));
            u.writeRow(Arrays.asList("1", "1", "f", "f", "2005-05-25T20:57:00Z"));
            u.writeRow(Arrays.asList("2", "2", "unm", "f", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("3", "3", "unm", "unm", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("5", "5", "unm", "unm", "2005-05-26T20:57:00Z"));
            u.commit();
        }

        checkResult("SELECT * FROM `multiPKUpdate` ORDER BY id1, id2, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "1", "d", "d", "0", "2005-05-23 20:57:00.000000", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "1", "a", "a", "0", "2005-05-24 20:57:00.000000", "2005-05-25 20:56:59.999999"),
                Arrays.asList("1", "1", "f", "f", "1", "2005-05-25 20:57:00.000000", "9999-12-31 23:59:59.999999"),
                Arrays.asList("1", "2", "a", "a", "1", "2005-05-24 20:57:00.000000", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "2", "e", "e", "0", "2005-05-23 20:57:00.000000", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "2", "b", "b", "0", "2005-05-24 20:57:00.000000", "2005-05-26 20:56:59.999999"),
                Arrays.asList("2", "2", "b", "f", "1", "2005-05-26 20:57:00.000000", "9999-12-31 23:59:59.999999"),
                Arrays.asList("3", "3", "c", "c", "0", "2005-05-24 20:57:00.000000", "2005-05-26 20:56:59.999999"),
                Arrays.asList("3", "3", "c", "c", "1", "2005-05-26 20:57:00.000000", "9999-12-31 23:59:59.999999")
        ));
    }

    @Test
    public void updateSameID() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE updateSameID(" +
                    "id INT, " +
                    "data TEXT, " +
                    "_fivetran_active BOOL, " +
                    "_fivetran_start DATETIME(6)," +
                    "_fivetran_end DATETIME(6)," +
                    "PRIMARY KEY(id, _fivetran_start))");
            stmt.execute("INSERT INTO updateSameID VALUES(1, 'a', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");

            Table t = JDBCUtil.getTable(conf, database, "updateSameID", "updateSameID", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            u.setHeader(Arrays.asList("id", "data", "_fivetran_start"));
            u.writeRow(Arrays.asList("1", "f", "2005-05-27T20:57:00Z"));
            u.writeRow(Arrays.asList("1", "unm", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("1", "ff", "2005-05-25T20:57:00Z"));
            u.commit();
        }

        checkResult("SELECT * FROM `updateSameID` ORDER BY id, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "a", "0", "2005-05-24 20:57:00.000000", "2005-05-25 20:56:59.999999"),
                Arrays.asList("1", "ff", "0", "2005-05-25 20:57:00.000000", "2005-05-26 20:56:59.999999"),
                Arrays.asList("1", "ff", "0", "2005-05-26 20:57:00.000000", "2005-05-27 20:56:59.999999"),
                Arrays.asList("1", "f", "1", "2005-05-27 20:57:00.000000", "9999-12-31 23:59:59.999999")
        ));
    }
}
