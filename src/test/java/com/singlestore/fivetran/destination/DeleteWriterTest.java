package com.singlestore.fivetran.destination;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.singlestore.fivetran.destination.writers.DeleteWriter;
import com.singlestore.fivetran.destination.writers.LoadDataWriter;

import fivetran_sdk.CsvFileParams;
import fivetran_sdk.Table;

public class DeleteWriterTest extends IntegrationTestBase {
    @Test
    public void allTypesTest() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE `%s`", database));
            stmt.execute("CREATE ROWSTORE TABLE `allTypesTableBigKey` (\n" + //
                    "  `id` INTEGER,\n" + //
                    "  `boolColumn` tinyint(1) DEFAULT NULL,\n" + //
                    "  `booleanColumn` tinyint(1) DEFAULT NULL,\n" + //
                    "  `bitColumn` bit(64) DEFAULT NULL,\n" + //
                    "  `tinyintColumn` tinyint(4) DEFAULT NULL,\n" + //
                    "  `smallintColumn` smallint(6) DEFAULT NULL,\n" + //
                    "  `mediumintColumn` mediumint(9) DEFAULT NULL,\n" + //
                    "  `intColumn` int(11) DEFAULT NULL,\n" + //
                    "  `integerColumn` int(11) DEFAULT NULL,\n" + //
                    "  `bigintColumn` bigint(20) DEFAULT NULL,\n" + //
                    "  `floatColumn` float DEFAULT NULL,\n" + //
                    "  `doubleColumn` double DEFAULT NULL,\n" + //
                    "  `realColumn` double DEFAULT NULL,\n" + //
                    "  `dateColumn` date DEFAULT NULL,\n" + //
                    "  `timeColumn` time DEFAULT NULL,\n" + //
                    "  `time6Column` time(6) DEFAULT NULL,\n" + //
                    "  `datetimeColumn` datetime DEFAULT NULL,\n" + //
                    "  `datetime6Column` datetime(6) DEFAULT NULL,\n" + //
                    "  `timestampColumn` timestamp NULL DEFAULT NULL,\n" + //
                    "  `timestamp6Column` timestamp(6) NULL DEFAULT NULL,\n" + //
                    "  `yearColumn` year(4) DEFAULT NULL,\n" + //
                    "  `decimalColumn` decimal(65,30) DEFAULT NULL,\n" + //
                    "  `decColumn` decimal(10,0) DEFAULT NULL,\n" + //
                    "  `fixedColumn` decimal(10,0) DEFAULT NULL,\n" + //
                    "  `numericColumn` decimal(10,0) DEFAULT NULL,\n" + //
                    "  `charColumn` char(1) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,\n"
                    + //
                    "  `mediumtextColumn` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci,\n"
                    + //
                    "  `binaryColumn` binary(1) DEFAULT NULL,\n" + //
                    "  `varcharColumn` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,\n"
                    + //
                    "  `varbinaryColumn` varbinary(100) DEFAULT NULL,\n" + //
                    "  `longtextColumn` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,\n" + //
                    "  `textColumn` text CHARACTER SET utf8 COLLATE utf8_general_ci,\n" + //
                    "  `tinytextColumn` tinytext CHARACTER SET utf8 COLLATE utf8_general_ci,\n" + //
                    "  `longblobColumn` longblob,\n" + //
                    "  `mediumblobColumn` mediumblob,\n" + //
                    "  `blobColumn` blob,\n" + //
                    "  `tinyblobColumn` tinyblob,\n" + //
                    "  `jsonColumn` JSON COLLATE utf8_bin,\n" + //
                    "  `geographyColumn` geography DEFAULT NULL,\n" + //
                    "  `geographypointColumn` geographypoint,\n" + //
                    "  PRIMARY KEY (\n" + //
                    "    `id`,\n" + //
                    "    `boolColumn`,\n" + //
                    "    `bitColumn`,\n" + //
                    "    `tinyintColumn`,\n" + //
                    "    `smallintColumn`,\n" + //
                    "    `mediumintColumn`,\n" + //
                    "    `intColumn`,\n" + //
                    "    `bigintColumn`,\n" + //
                    "    `dateColumn`,\n" + //
                    "    `timeColumn`,\n" + //
                    "    `time6Column`,\n" + //
                    "    `datetimeColumn`,\n" + //
                    "    `datetime6Column`,\n" + //
                    "    `timestampColumn`,\n" + //
                    "    `timestamp6Column`,\n" + //
                    "    `yearColumn`,\n" + //
                    "    `decimalColumn`,\n" + //
                    "    `charColumn`,\n" + //
                    "    `mediumtextColumn`,\n" + //
                    "    `binaryColumn`,\n" + //
                    "    `varcharColumn`,\n" + //
                    "    `varbinaryColumn`,\n" + //
                    "    `longtextColumn`,\n" + //
                    "    `textColumn`,\n" + //
                    "    `tinytextColumn`,\n" + //
                    "    `longblobColumn`,\n" + //
                    "    `mediumblobColumn`,\n" + //
                    "    `blobColumn`,\n" + //
                    "    `tinyblobColumn`,\n" + //
                    "    `geographypointColumn`\n" + //
                    "  )\n" + //
                    ") AUTOSTATS_CARDINALITY_MODE=PERIODIC AUTOSTATS_HISTOGRAM_MODE=CREATE SQL_MODE='STRICT_ALL_TABLES'");

            Table allTypesTableBigKey = JDBCUtil.getTable(conf, database, "allTypesTableBigKey");
            CsvFileParams params = CsvFileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w =
                    new LoadDataWriter(conn, database, allTypesTableBigKey, params, null);
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

            DeleteWriter d = new DeleteWriter(conn, database, allTypesTableBigKey, params, null);
            d.setHeader(allTypesColumns);
            d.writeRow(List.of("1", "FALSE", "false", "", "-128", "-32768", "-8388608",
                    "-2147483648", "-2147483648", "-9223372036854775808", "-100.1", "-1000.01",
                    "-1000.01", "1000-01-01", "-838:59:59", "-838:59:59.000000",
                    "1000-01-01T00:00:00", "1000-01-01T00:00:00.000Z", "1970-01-01T00:00:01",
                    "1970-01-01T00:00:01.000Z", "1901",
                    "-12345678901234567890123456789012345.123456789012345678901234567891",
                    "123456789", "123456789", "123456789", "a", "abc", "a", "abc", "abc", "abc",
                    "abc", "abc", "abc", "abc", "abc", "abc", "{\"a\":\"b\"}",
                    "POLYGON((0 0, 0 1, 1 1, 0 0))", "POINT(-74.044514 40.689244)"));
            d.commit();
        }

        checkResult("SELECT * FROM `allTypesTableBigKey` ORDER BY id", Arrays.asList());
    }

    @Test
    public void deletePartOfRows() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE deletePartOfRows(id INT PRIMARY KEY, a INT, b INT)");
            stmt.execute("INSERT INTO deletePartOfRows VALUES(1, 2, 3)");
            stmt.execute("INSERT INTO deletePartOfRows VALUES(4, 5, 6)");
            stmt.execute("INSERT INTO deletePartOfRows VALUES(7, 8, 9)");
            stmt.execute("INSERT INTO deletePartOfRows VALUES(10, 11, 12)");

            Table t = JDBCUtil.getTable(conf, database, "deletePartOfRows");
            CsvFileParams params = CsvFileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            DeleteWriter d = new DeleteWriter(conn, database, t, params, null);
            d.setHeader(List.of("id", "a", "b"));
            d.writeRow(List.of("4", "unm", "unm"));
            d.writeRow(List.of("7", "unm", "unm"));
            d.writeRow(List.of("100", "unm", "unm"));
            d.commit();
        }

        checkResult("SELECT * FROM `deletePartOfRows` ORDER BY id",
                Arrays.asList(Arrays.asList("1", "2", "3"), Arrays.asList("10", "11", "12")));
    }

    @Test
    public void bigDelete() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE bigDelete(id INT PRIMARY KEY)");

            CsvFileParams params = CsvFileParams.newBuilder().setNullString("NULL").build();
            Table t = JDBCUtil.getTable(conf, database, "bigDelete");
            LoadDataWriter w = new LoadDataWriter(conn, database, t, params, null);
            w.setHeader(List.of("id"));
            for (Integer i = 0; i < 20000; i++) {
                w.writeRow(List.of(i.toString()));
            }
            w.commit();

            DeleteWriter d = new DeleteWriter(conn, database, t, params, null);
            d.setHeader(List.of("id"));
            for (Integer i = 0; i < 10000; i++) {
                d.writeRow(List.of(i.toString()));
            }
            d.commit();
        }

        List<List<String>> res = new ArrayList<>();
        for (Integer i = 10000; i < 20000; i++) {
            res.add(Arrays.asList(i.toString()));
        }

        checkResult("SELECT * FROM `bigDelete` ORDER BY id", res);
    }
}
