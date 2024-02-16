package com.singlestore.fivetran.destination;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.singlestore.fivetran.destination.writers.LoadDataWriter;
import com.singlestore.fivetran.destination.writers.UpdateWriter;

import fivetran_sdk.CsvFileParams;
import fivetran_sdk.Table;

public class UpdateWriterTets extends IntegrationTestBase {
    @Test
    public void allTypes() throws Exception {
        createAllTypesTable();

        try (Connection conn = JDBCUtil.createConnection(conf)) {
            Table allTypesTable = JDBCUtil.getTable(conf, database, "allTypesTable");
            CsvFileParams params = CsvFileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w = new LoadDataWriter(conn, database, allTypesTable, params, null);
            w.setHeader(allTypesColumns);
            w.writeRow(List.of("1", "FALSE", "false", "", "-128", "-32768", "-8388608",
                    "-2147483648", "-2147483648", "-9223372036854775808", "-100.1", "-1000.01",
                    "-1000.01", "1000-01-01", "-838:59:59", "-838:59:59.000000",
                    "1000-01-01T00:00:00", "1000-01-01T00:00:00.000Z", "1970-01-01T00:00:01",
                    "1970-01-01T00:00:01.000Z", "1901",
                    "-12345678901234567890123456789012345.123456789012345678901234567891",
                    "123456789", "123456789", "123456789", "a", "abc", "a", "abc", "abc", "abc",
                    "abc", "abc", "abc", "abc", "abc", "abc", "{\"a\":\"b\"}",
                    "POLYGON((0 0, 0 1, 1 1, 0 0))", "POINT(-74.044514 40.689244)"));
            w.commit();

            UpdateWriter u = new UpdateWriter(conn, database, allTypesTable, params, null);
            u.setHeader(allTypesColumns);
            u.writeRow(List.of("1", "TRUE", "true", "12345678", "127", "32767", "8388607",
                    "2147483647", "2147483647", "9223372036854775807", "100.1", "1000.01",
                    "1000.01", "9999-12-31", "830:00:00", "830:00:00.000000", "9999-12-31T23:59:59",
                    "9999-12-31T23:59:59.999Z", "2038-01-19T03:14:07", "2038-01-19T03:14:07.999Z",
                    "2155", "12345678901234567890123456789012345.123456789012345678901234567891",
                    "123456789", "123456789", "123456789", "a", "abc", "a", "abc", "abc", "abc",
                    "abc", "abc", "abc", "abc", "abc", "abc", "{\"a\":\"b\"}",
                    "POLYGON((0 0, 0 1, 1 1, 0 0))", "POINT(-74.044514 40.689244)"));
            u.commit();
        }

        checkResult("SELECT * FROM `allTypesTable` ORDER BY id", Arrays.asList(Arrays.asList("1",
                "1", "1", "b'11000100110010001100110011010000110101001101100011011100111000'",
                "127", "32767", "8388607", "2147483647", "2147483647", "9223372036854775807",
                "100.1", "1000.01", "1000.01", "9999-12-31", "830:00:00", "830:00:00.000000",
                "9999-12-31 23:59:59", "9999-12-31 23:59:59.999000", "2038-01-19 03:14:07",
                "2038-01-19 03:14:07.999000", "2155",
                "12345678901234567890123456789012345.123456789012345678901234567891", "123456789",
                "123456789", "123456789", "a", "abc", "a", "abc", "abc", "abc", "abc", "abc", "abc",
                "abc", "abc", "abc", "{\"a\":\"b\"}",
                "POLYGON((1.00000000 1.00000000, 0.00000000 1.00000000, 0.00000000 0.00000000, 1.00000000 1.00000000))",
                "POINT(-74.04451396 40.68924403)")));
    }

    @Test
    public void partialUpdate() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE partialUpdate(id INT PRIMARY KEY, a INT, b INT)");
            stmt.execute("INSERT INTO partialUpdate VALUES(1, 2, 3)");
            stmt.execute("INSERT INTO partialUpdate VALUES(4, 5, 6)");
            stmt.execute("INSERT INTO partialUpdate VALUES(7, 8, 9)");
            stmt.execute("INSERT INTO partialUpdate VALUES(10, 11, 12)");

            Table t = JDBCUtil.getTable(conf, database, "partialUpdate");
            CsvFileParams params = CsvFileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            UpdateWriter u = new UpdateWriter(conn, database, t, params, null);
            u.setHeader(List.of("id", "a", "b"));
            u.writeRow(List.of("4", "unm", "1"));
            u.writeRow(List.of("7", "10", "unm"));
            u.writeRow(List.of("10", "unm", "unm"));
            u.writeRow(List.of("unm", "unm", "unm"));
            u.commit();
        }

        checkResult("SELECT * FROM `partialUpdate` ORDER BY id",
                Arrays.asList(Arrays.asList("1", "2", "3"), Arrays.asList("4", "5", "1"),
                        Arrays.asList("7", "10", "9"), Arrays.asList("10", "11", "12")));
    }
}
