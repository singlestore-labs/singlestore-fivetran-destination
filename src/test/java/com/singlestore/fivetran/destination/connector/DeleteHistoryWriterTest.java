package com.singlestore.fivetran.destination.connector;

import com.singlestore.fivetran.destination.connector.writers.*;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeleteHistoryWriterTest extends IntegrationTestBase {
    @Test
    public void noFivetranEnd() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE noFivetranEnd(" +
                    "id INT PRIMARY KEY, " +
                    "data TEXT, " +
                    "_fivetran_active BOOL, " +
                    "_fivetran_start DATETIME(6))");
            stmt.execute("INSERT INTO noFivetranEnd VALUES(1, 'a', TRUE, '2005-05-24 20:57:00.000000')");

            Table t = JDBCUtil.getTable(conf, database, "noFivetranEnd", "noFivetranEnd", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            DeleteHistoryWriter d = new DeleteHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            Exception exception = assertThrows(IllegalArgumentException.class, () -> {
                d.setHeader(Arrays.asList("id", "data", "_fivetran_active", "_fivetran_start"));
            });

            assertEquals("File doesn't contain _fivetran_end column", exception.getMessage());
        }
    }

    @Test
    public void singlePK() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE singlePK(" +
                    "id INT, " +
                    "data TEXT, " +
                    "_fivetran_active BOOL, " +
                    "_fivetran_start DATETIME(6)," +
                    "_fivetran_end DATETIME(6)," +
                    "PRIMARY KEY(id, _fivetran_start))");
            stmt.execute("INSERT INTO singlePK VALUES(1, 'a', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO singlePK VALUES(2, 'b', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO singlePK VALUES(3, 'c', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO singlePK VALUES(1, 'd', FALSE, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO singlePK VALUES(2, 'e', FALSE, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");

            Table t = JDBCUtil.getTable(conf, database, "singlePK", "singlePK", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            DeleteHistoryWriter d = new DeleteHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            d.setHeader(Arrays.asList("id", "_fivetran_end"));
            d.writeRow(Arrays.asList("1", "2005-05-25T20:57:00Z"));
            d.writeRow(Arrays.asList("2", "2005-05-26T20:57:00Z"));
            d.writeRow(Arrays.asList("5", "2005-05-26T20:57:00Z"));
            d.commit();
        }

        checkResult("SELECT * FROM `singlePK` ORDER BY id, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "d", "0", "2005-05-23 20:57:00.000000", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "a", "0", "2005-05-24 20:57:00.000000", "2005-05-25 20:57:00.000000"),
                Arrays.asList("2", "e", "0", "2005-05-23 20:57:00.000000", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "b", "0", "2005-05-24 20:57:00.000000", "2005-05-26 20:57:00.000000"),
                Arrays.asList("3", "c", "1", "2005-05-24 20:57:00.000000", "9999-12-31 23:59:59.999999")
        ));
    }

    @Test
    public void multiPK() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE multiPK(" +
                    "id1 INT, " +
                    "id2 TEXT, " +
                    "data TEXT, " +
                    "_fivetran_active BOOL, " +
                    "_fivetran_start DATETIME(6)," +
                    "_fivetran_end DATETIME(6)," +
                    "PRIMARY KEY(id1, id2, _fivetran_start))");
            stmt.execute("INSERT INTO multiPK VALUES(1, 2, 'a', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO multiPK VALUES(1, 1, 'a', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO multiPK VALUES(2, 2, 'b', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO multiPK VALUES(3, 3, 'c', TRUE, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO multiPK VALUES(1, 1, 'd', FALSE, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO multiPK VALUES(2, 2, 'e', FALSE, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");

            Table t = JDBCUtil.getTable(conf, database, "multiPK", "multiPK", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            DeleteHistoryWriter d = new DeleteHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            d.setHeader(Arrays.asList("id1", "id2", "_fivetran_end"));
            d.writeRow(Arrays.asList("1", "1", "2005-05-25T20:57:00Z"));
            d.writeRow(Arrays.asList("2", "2", "2005-05-26T20:57:00Z"));
            d.writeRow(Arrays.asList("5", "5", "2005-05-26T20:57:00Z"));
            d.commit();
        }

        checkResult("SELECT * FROM `multiPK` ORDER BY id1, id2, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "1", "d", "0", "2005-05-23 20:57:00.000000", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "1", "a", "0", "2005-05-24 20:57:00.000000", "2005-05-25 20:57:00.000000"),
                Arrays.asList("1", "2", "a", "1", "2005-05-24 20:57:00.000000", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "2", "e", "0", "2005-05-23 20:57:00.000000", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "2", "b", "0", "2005-05-24 20:57:00.000000", "2005-05-26 20:57:00.000000"),
                Arrays.asList("3", "3", "c", "1", "2005-05-24 20:57:00.000000", "9999-12-31 23:59:59.999999")
        ));
    }
}
