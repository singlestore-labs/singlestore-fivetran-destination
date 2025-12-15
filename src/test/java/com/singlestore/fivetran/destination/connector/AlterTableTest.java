package com.singlestore.fivetran.destination.connector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AlterTableTest extends IntegrationTestBase {

    @Test
    public void addColumn() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE addColumn(a INT)");
            Table table = Table.newBuilder().setName("addColumn")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.INT).build(),
                            Column.newBuilder().setName("b").setType(DataType.INT).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }
            Table result = JDBCUtil.getTable(conf, database, "addColumn", "addColumn", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.INT, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());
        }
    }

    @Test
    public void changeDataType() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE changeDataType(a INT)");
            stmt.execute("INSERT INTO changeDataType VALUES (5)");

            Table table = Table.newBuilder().setName("changeDataType")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.STRING).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }
            Table result = JDBCUtil.getTable(conf, database, "changeDataType", "changeDataType", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.STRING, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            checkResult("SELECT * FROM `changeDataType`", Arrays.asList(Arrays.asList("5")));
        }
    }

    @Test
    public void changeKey() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE changeKey(a INT)");
            Table table = Table.newBuilder().setName("changeKey").addAllColumns(Arrays.asList(Column
                            .newBuilder().setName("a").setType(DataType.INT).setPrimaryKey(true).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }
            Table result = JDBCUtil.getTable(conf, database, "changeKey", "changeKey", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

        }
    }

    @Test
    public void severalOperations() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE severalOperations(a INT, b INT)");
            stmt.execute("INSERT INTO severalOperations VALUES (5, 6)");

            Table table = Table.newBuilder().setName("severalOperations")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.STRING).build(),
                            Column.newBuilder().setName("b").setType(DataType.LONG).build(),
                            Column.newBuilder().setName("c").setType(DataType.LONG).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }
            Table result =
                    JDBCUtil.getTable(conf, database, "severalOperations", "severalOperations", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.STRING, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.LONG, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());

            assertEquals("c", columns.get(2).getName());
            assertEquals(DataType.LONG, columns.get(2).getType());
            assertEquals(false, columns.get(2).getPrimaryKey());

            checkResult("SELECT * FROM `severalOperations`",
                    Arrays.asList(Arrays.asList("5", "6", null)));
        }
    }

    @Test
    public void changeScaleAndPrecision() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE changeScaleAndPrecision(a DECIMAL(38, 30))");
            stmt.execute("INSERT INTO changeScaleAndPrecision VALUES ('5.123')");

            Table table = Table.newBuilder().setName("changeScaleAndPrecision").addAllColumns(
                            Arrays.asList(Column.newBuilder().setName("a").setType(DataType.DECIMAL)
                                    .setParams(DataTypeParams.newBuilder()
                                            .setDecimal(DecimalParams.newBuilder()
                                                    .setScale(5)
                                                    .setPrecision(10))
                                            .build())
                                    .build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }
            Table result = JDBCUtil.getTable(conf, database, "changeScaleAndPrecision",
                    "changeScaleAndPrecision", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.DECIMAL, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());
            assertEquals(10, columns.get(0).getParams().getDecimal().getPrecision());
            assertEquals(5, columns.get(0).getParams().getDecimal().getScale());

            checkResult("SELECT * FROM `changeScaleAndPrecision`",
                    Arrays.asList(Arrays.asList("5.12300")));
        }
    }

    @Test
    public void shouldIgnoreDifferentDatetimeColumns() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            Table naiveDatetimeTable =
                    Table.newBuilder().setName("shouldIgnoreDifferentDatetimeColumns")
                            .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                                    .setType(DataType.NAIVE_DATETIME).build()))
                            .build();

            Table utcDatetimeTable =
                    Table.newBuilder().setName("shouldIgnoreDifferentDatetimeColumns")
                            .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                                    .setType(DataType.UTC_DATETIME).build()))
                            .build();

            CreateTableRequest createRequest = CreateTableRequest.newBuilder()
                    .setSchemaName(database).setTable(naiveDatetimeTable).build();

            String query = JDBCUtil.generateCreateTableQuery(conf, stmt, createRequest);
            stmt.execute(query);

            AlterTableRequest alterRequest =
                    AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                            .setSchemaName(database).setTable(utcDatetimeTable).build();

            List<JDBCUtil.QueryWithCleanup> alterQuery = JDBCUtil.generateAlterTableQuery(alterRequest, testWarningHandle);
            assertNull(alterQuery);
        }
    }

    @Test
    public void changeTypeOfKey() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(
                    String.format("CREATE TABLE %s.changeTypeOfKey(a INT PRIMARY KEY)", database));
            stmt.execute(String.format("INSERT INTO %s.changeTypeOfKey VALUES (1), (2)", database));
            Table table = Table.newBuilder().setName("changeTypeOfKey")
                    .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                            .setType(DataType.LONG).setPrimaryKey(true).build()))
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("b").setType(DataType.LONG).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table result = JDBCUtil.getTable(conf, database, "changeTypeOfKey", "changeTypeOfKey", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.LONG, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.LONG, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());

            checkResult(String.format("SELECT * FROM %s.`changeTypeOfKey` ORDER BY a", database),
                    Arrays.asList(Arrays.asList("1", null), Arrays.asList("2", null)));
        }
    }

    @Test
    public void changeTypeOfKeyCleanup() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(
                    String.format("CREATE TABLE %s.changeTypeOfKeyCleanup(a INT PRIMARY KEY)", database));
            stmt.execute(String.format("INSERT INTO %s.changeTypeOfKeyCleanup VALUES (1), (2)", database));
            Table table = Table.newBuilder().setName("changeTypeOfKeyCleanup")
                    .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                            .setType(DataType.LONG).setPrimaryKey(true).build()))
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("b").setType(DataType.LONG).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            stmt.execute(queries.get(0).getQuery());
            stmt.execute(queries.get(1).getCleanupQuery());

            Table result = JDBCUtil.getTable(conf, database, "changeTypeOfKeyCleanup", "changeTypeOfKeyCleanup", testWarningHandle);
            List<Column> columns = result.getColumnsList();
            assertEquals(1, columns.size());

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            checkResult(String.format("SELECT * FROM %s.`changeTypeOfKeyCleanup` ORDER BY a", database),
                    Arrays.asList(Arrays.asList("1"), Arrays.asList("2")));

            queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            stmt.execute(queries.get(0).getQuery());
            stmt.execute(queries.get(1).getQuery());
            stmt.execute(queries.get(2).getCleanupQuery());

            result = JDBCUtil.getTable(conf, database, "changeTypeOfKeyCleanup", "changeTypeOfKeyCleanup", testWarningHandle);
            columns = result.getColumnsList();
            assertEquals(1, columns.size());

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            checkResult(String.format("SELECT * FROM %s.`changeTypeOfKeyCleanup` ORDER BY a", database),
                    Arrays.asList(Arrays.asList("1"), Arrays.asList("2")));

        }
    }

    @Test
    public void changeTypeCleanup() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(
                    String.format("CREATE TABLE %s.changeTypeCleanup(a INT)", database));
            stmt.execute(String.format("INSERT INTO %s.changeTypeCleanup VALUES (1), (2)", database));
            Table table = Table.newBuilder().setName("changeTypeCleanup")
                    .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                            .setType(DataType.LONG).setPrimaryKey(true).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            stmt.execute(queries.get(0).getQuery());
            stmt.execute(queries.get(1).getCleanupQuery());

            Table result = JDBCUtil.getTable(conf, database, "changeTypeCleanup", "changeTypeCleanup", testWarningHandle);
            List<Column> columns = result.getColumnsList();
            assertEquals(1, columns.size());

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            checkResult(String.format("SELECT * FROM %s.`changeTypeCleanup` ORDER BY a", database),
                    Arrays.asList(Arrays.asList("1"), Arrays.asList("2")));

            queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            stmt.execute(queries.get(0).getQuery());
            stmt.execute(queries.get(1).getQuery());
            stmt.execute(queries.get(2).getCleanupQuery());

            result = JDBCUtil.getTable(conf, database, "changeTypeCleanup", "changeTypeCleanup", testWarningHandle);
            columns = result.getColumnsList();
            assertEquals(1, columns.size());

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            checkResult(String.format("SELECT * FROM %s.`changeTypeCleanup` ORDER BY a", database),
                    Arrays.asList(Arrays.asList("1"), Arrays.asList("2")));

        }
    }

    @Test
    public void dropColumn() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE dropColumn(a INT, b INT)");
            Table table = Table.newBuilder().setName("dropColumn")
                    .addAllColumns(Collections.singletonList(
                            Column.newBuilder().setName("a").setType(DataType.INT).build()
                    ))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).setDropColumns(true).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }
            Table result = JDBCUtil.getTable(conf, database, "dropColumn", "dropColumn", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertFalse(columns.get(0).getPrimaryKey());
            assertEquals(1, columns.size());
        }
    }

    @Test
    public void dropPK() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE dropPK(a INT, b INT PRIMARY KEY)");
            Table table = Table.newBuilder().setName("dropPK")
                    .addAllColumns(Collections.singletonList(
                            Column.newBuilder().setName("a").setType(DataType.INT).build()
                    ))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).setDropColumns(true).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }
            Table result = JDBCUtil.getTable(conf, database, "dropPK", "dropPK", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertFalse(columns.get(0).getPrimaryKey());
            assertEquals(1, columns.size());
        }
    }

    @Test
    public void dontDrop() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE dontDrop(a INT, b INT)");
            Table table = Table.newBuilder().setName("dontDrop")
                    .addAllColumns(Collections.singletonList(
                            Column.newBuilder().setName("a").setType(DataType.INT).build()
                    ))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            assertNull(queries);
        }
    }
}
