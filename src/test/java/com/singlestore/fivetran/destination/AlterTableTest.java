package com.singlestore.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import fivetran_sdk.AlterTableRequest;
import fivetran_sdk.Column;
import fivetran_sdk.CreateTableRequest;
import fivetran_sdk.DataType;
import fivetran_sdk.DecimalParams;
import fivetran_sdk.Table;

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

            String query = JDBCUtil.generateAlterTableQuery(request);
            stmt.execute(query);
            Table result = JDBCUtil.getTable(conf, database, "addColumn", "addColumn");
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

            String query = JDBCUtil.generateAlterTableQuery(request);
            stmt.execute(query);
            Table result = JDBCUtil.getTable(conf, database, "changeDataType", "changeDataType");
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

            String query = JDBCUtil.generateAlterTableQuery(request);
            stmt.execute(query);
            Table result = JDBCUtil.getTable(conf, database, "changeTypeOfKey", "changeTypeOfKey");
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

            String query = JDBCUtil.generateAlterTableQuery(request);
            stmt.execute(query);
            Table result =
                    JDBCUtil.getTable(conf, database, "severalOperations", "severalOperations");
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
                            .setDecimal(DecimalParams.newBuilder().setScale(5).setPrecision(10))
                            .build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            String query = JDBCUtil.generateAlterTableQuery(request);
            stmt.execute(query);
            Table result = JDBCUtil.getTable(conf, database, "changeScaleAndPrecision",
                    "changeScaleAndPrecision");
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.DECIMAL, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());
            assertEquals(10, columns.get(0).getDecimal().getPrecision());
            assertEquals(5, columns.get(0).getDecimal().getScale());

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

            query = JDBCUtil.generateAlterTableQuery(alterRequest);
            assertNull(query);
        }
    }

    @Test
    public void changeTypeOfKey() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE changeTypeOfKey(a INT PRIMARY KEY)");
            stmt.execute("INSERT INTO changeTypeOfKey VALUES (1), (2)");
            Table table = Table.newBuilder().setName("changeTypeOfKey")
                    .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                            .setType(DataType.LONG).setPrimaryKey(true).build()))
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("b").setType(DataType.LONG).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            String query = JDBCUtil.generateAlterTableQuery(request);

            stmt.execute(query);
            Table result = JDBCUtil.getTable(conf, database, "changeTypeOfKey", "changeTypeOfKey");
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.LONG, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.LONG, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());

            checkResult("SELECT * FROM `changeTypeOfKey` ORDER BY a",
                    Arrays.asList(Arrays.asList("1", null), Arrays.asList("2", null)));
        }
    }
}
