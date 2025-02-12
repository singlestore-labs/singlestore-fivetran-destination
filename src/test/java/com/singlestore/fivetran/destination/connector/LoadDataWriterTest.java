package com.singlestore.fivetran.destination.connector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.singlestore.fivetran.destination.connector.writers.LoadDataWriter;

import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;

public class LoadDataWriterTest extends IntegrationTestBase {
    @Test
    public void allTypes() throws Exception {
        createAllTypesTable();

        try (Connection conn = JDBCUtil.createConnection(conf)) {
            Table allTypesTable =
                    JDBCUtil.getTable(conf, database, "allTypesTable", "allTypesTable", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w = new LoadDataWriter(conn, database, allTypesTable.getName(),
                    allTypesTable.getColumnsList(), params, null, 123, testWarningHandle);
            w.setHeader(List.of("id", "boolColumn", "booleanColumn", "bitColumn", "tinyintColumn",
                    "smallintColumn", "mediumintColumn", "intColumn", "integerColumn",
                    "bigintColumn", "floatColumn", "doubleColumn", "realColumn", "dateColumn",
                    "timeColumn", "time6Column", "datetimeColumn", "datetime6Column",
                    "timestampColumn", "timestamp6Column", "yearColumn", "decimalColumn",
                    "decColumn", "fixedColumn", "numericColumn", "charColumn", "mediumtextColumn",
                    "binaryColumn", "varcharColumn", "varbinaryColumn", "longtextColumn",
                    "textColumn", "tinytextColumn", "longblobColumn", "mediumblobColumn",
                    "blobColumn", "tinyblobColumn", "jsonColumn", "geographyColumn",
                    "geographypointColumn"));
            w.writeRow(List.of("1", "FALSE", "false", "", "-128", "-32768", "-8388608",
                    "-2147483648", "-2147483648", "-9223372036854775808", "-100.1", "-1000.01",
                    "-1000.01", "1000-01-01", "-838:59:59", "-838:59:59.000000",
                    "1000-01-01T00:00:00", "1000-01-01T00:00:00.000Z", "1970-01-01T00:00:01",
                    "1970-01-01T00:00:01.000Z", "1901",
                    "-12345678901234567890123456789012345.123456789012345678901234567891",
                    "123456789", "123456789", "123456789", "a", "abc", "YQ==", "abc", "YWJj", "abc",
                    "abc", "abc", "YWJj", "YWJj", "YWJj", "YWJj", "{\"a\":\"b\"}",
                    "POLYGON((0 0, 0 1, 1 1, 0 0))", "POINT(-74.044514 40.689244)"));
            w.writeRow(List.of("2", "TRUE", "true", "MTIzNDU2Nzg=", "127", "32767", "8388607",
                    "2147483647", "2147483647", "9223372036854775807", "100.1", "1000.01",
                    "1000.01", "9999-12-31", "830:00:00", "830:00:00.000000", "9999-12-31T23:59:59",
                    "9999-12-31T23:59:59.999Z", "2038-01-19T03:14:07", "2038-01-19T03:14:07.999Z",
                    "2155", "12345678901234567890123456789012345.123456789012345678901234567891",
                    "123456789", "123456789", "123456789", "a", "abc", "YQ==", "abc", "YWJj", "abc",
                    "abc", "abc", "YWJj", "YWJj", "YWJj", "YWJj", "{\"a\":\"b\"}",
                    "POLYGON((0 0, 0 1, 1 1, 0 0))", "POINT(-74.044514 40.689244)"));
            w.writeRow(List.of("3", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
                    "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
                    "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
                    "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
                    "NULL"));
            w.commit();
        }


        checkResult("SELECT * FROM `allTypesTable` ORDER BY id", Arrays.asList(Arrays.asList("1",
                        "0", "0", "b''", "-128", "-32768", "-8388608", "-2147483648", "-2147483648",
                        "-9223372036854775808", "-100.1", "-1000.01", "-1000.01", "1000-01-01",
                        "-838:59:59", "-838:59:59.000000", "1000-01-01 00:00:00",
                        "1000-01-01 00:00:00.000000", "1970-01-01 00:00:01", "1970-01-01 00:00:01.000000",
                        "1901", "-12345678901234567890123456789012345.123456789012345678901234567891",
                        "123456789", "123456789", "123456789", "a", "abc", "a", "abc", "abc", "abc", "abc",
                        "abc", "abc", "abc", "abc", "abc", "{\"a\":\"b\"}",
                        "POLYGON((1.00000000 1.00000000, 0.00000000 1.00000000, 0.00000000 0.00000000, 1.00000000 1.00000000))",
                        "POINT(-74.04451396 40.68924403)"),
                Arrays.asList("2", "1", "1",
                        "b'11000100110010001100110011010000110101001101100011011100111000'", "127",
                        "32767", "8388607", "2147483647", "2147483647", "9223372036854775807",
                        "100.1", "1000.01", "1000.01", "9999-12-31", "830:00:00",
                        "830:00:00.000000", "9999-12-31 23:59:59", "9999-12-31 23:59:59.999000",
                        "2038-01-19 03:14:07", "2038-01-19 03:14:07.999000", "2155",
                        "12345678901234567890123456789012345.123456789012345678901234567891",
                        "123456789", "123456789", "123456789", "a", "abc", "a", "abc", "abc", "abc",
                        "abc", "abc", "abc", "abc", "abc", "abc", "{\"a\":\"b\"}",
                        "POLYGON((1.00000000 1.00000000, 0.00000000 1.00000000, 0.00000000 0.00000000, 1.00000000 1.00000000))",
                        "POINT(-74.04451396 40.68924403)"),
                Arrays.asList("3", null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null)));
    }

    @Test
    public void allBytes() throws Exception {
        byte[] data = new byte[256];
        for (int i = 0; i < 256; i++) {
            data[i] = (byte) i;
        }

        String dataBase64 = Base64.getEncoder().encodeToString(data);
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.executeQuery("CREATE TABLE allBytes(a BLOB)");
            Table allBytesTable = JDBCUtil.getTable(conf, database, "allBytes", "allBytes", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w = new LoadDataWriter(conn, database, allBytesTable.getName(),
                    allBytesTable.getColumnsList(), params, null, 123);
            w.setHeader(List.of("a"));
            w.writeRow(List.of(dataBase64));
            w.commit();

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM allBytes")) {
                assertTrue(rs.next());
                assertArrayEquals(data, rs.getBytes(1));
                assertFalse(rs.next());
            }
        }
    }
}
