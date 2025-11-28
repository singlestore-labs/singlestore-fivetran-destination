package com.singlestore.fivetran.destination.connector;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MigrateTest extends IntegrationTestBase {
    @Test
    public void dropTable() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE dropTable(a INT)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("dropTable")
                            .setSchema(database)
                            .setDrop(
                                    DropOperation.newBuilder()
                                            .setDropTable(true)
                            ))
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Assertions.assertThrows(JDBCUtil.TableNotExistException.class, () -> {
                JDBCUtil.getTable(conf, database, "dropTable", "dropTable", testWarningHandle);
            });
        }
    }

    @Test
    public void renameTable() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE renameTable(a INT)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("renameTable")
                            .setSchema(database)
                            .setRename(
                                    RenameOperation.newBuilder()
                                            .setRenameTable(
                                                    RenameTable.newBuilder()
                                                            .setFromTable("renameTable")
                                                            .setToTable("renameTable1")
                                            )
                            ))
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table renamed = JDBCUtil.getTable(conf, database, "renameTable1", "renameTable1", testWarningHandle);
            Assertions.assertEquals("renameTable1", renamed.getName());
        }
    }

    @Test
    public void renameColumn() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE renameColumn(a INT)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("renameColumn")
                            .setSchema(database)
                            .setRename(
                                    RenameOperation.newBuilder()
                                            .setRenameColumn(
                                                    RenameColumn.newBuilder()
                                                            .setFromColumn("a")
                                                            .setToColumn("b")
                                            )
                            ))
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table renamed = JDBCUtil.getTable(conf, database, "renameColumn", "renameTable1", testWarningHandle);
            renamed.getColumnsList().forEach(c -> Assertions.assertEquals("b", c.getName()));
        }
    }

    @Test
    public void copyTable() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE copyTable(a INT)");
            stmt.execute("INSERT INTO copyTable VALUES (1), (2), (3)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyTable")
                            .setSchema(database)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyTable(
                                                    CopyTable.newBuilder()
                                                            .setFromTable("copyTable")
                                                            .setToTable("copyTable1")
                                            )
                            ))
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table originalTable = JDBCUtil.getTable(conf, database, "copyTable", "copyTable", testWarningHandle);
            Assertions.assertEquals("copyTable", originalTable.getName());

            Table copy = JDBCUtil.getTable(conf, database, "copyTable1", "copyTable1", testWarningHandle);
            Assertions.assertEquals("copyTable1", copy.getName());

            checkResult("SELECT * FROM `copyTable1` ORDER BY a",
                    Arrays.asList(Collections.singletonList("1"),
                            Collections.singletonList("2"),
                            Collections.singletonList("3")));
        }
    }

    @Test
    public void copyColumn() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE copyColumn(a DECIMAL(8, 4))");
            stmt.execute("INSERT INTO copyColumn VALUES (123), (124)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyColumn")
                            .setSchema(database)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyColumn(
                                                    CopyColumn.newBuilder()
                                                            .setFromColumn("a")
                                                            .setToColumn("b")
                                            )
                            ))
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table copy = JDBCUtil.getTable(conf, database, "copyColumn", "copyColumn", testWarningHandle);
            List<Column> columns = copy.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(1).getName());
            Assertions.assertEquals(8, columns.get(1).getParams().getDecimal().getPrecision());
            Assertions.assertEquals(4, columns.get(1).getParams().getDecimal().getScale());

            checkResult("SELECT a, b FROM copyColumn ORDER BY a", Arrays.asList(
                    Arrays.asList("123.0000", "123.0000"),
                    Arrays.asList("124.0000", "124.0000")
            ));
        }
    }

    @Test
    public void addColumnWithDefaultValue() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE addColumnWithDefaultValue(a INT)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("addColumnWithDefaultValue")
                            .setSchema(database)
                            .setAdd(AddOperation.newBuilder()
                                    .setAddColumnWithDefaultValue(
                                            AddColumnWithDefaultValue.newBuilder()
                                                    .setColumn("b")
                                                    .setDefaultValue("1")
                                                    .setColumnType(DataType.INT)
                                                    .build()
                                    )
                            )
                    )
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            Table t = JDBCUtil.getTable(conf, database, "addColumnWithDefaultValue", "addColumnWithDefaultValue", testWarningHandle);
            Optional<Column> optionalB = t.getColumnsList().stream().filter(c -> c.getName().equals("b")).findFirst();
            Assertions.assertTrue(optionalB.isPresent());
            Column b = optionalB.get();
            Assertions.assertEquals("b", b.getName());
            Assertions.assertEquals(DataType.INT, b.getType());

            request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("addColumnWithDefaultValue")
                            .setSchema(database)
                            .setAdd(AddOperation.newBuilder()
                                    .setAddColumnWithDefaultValue(
                                            AddColumnWithDefaultValue.newBuilder()
                                                    .setColumn("c")
                                                    .setDefaultValue("2025-11-24T10:14:54.123Z")
                                                    .setColumnType(DataType.NAIVE_DATETIME)
                                                    .build()
                                    )
                            )
                    )
                    .build();

            queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            t = JDBCUtil.getTable(conf, database, "addColumnWithDefaultValue", "addColumnWithDefaultValue", testWarningHandle);
            Optional<Column> optionalC = t.getColumnsList().stream().filter(c -> c.getName().equals("c")).findFirst();
            Assertions.assertTrue(optionalC.isPresent());
            Column c = optionalC.get();
            Assertions.assertEquals("c", c.getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, c.getType());
        }
    }

    @Test
    public void updateColumnValueOperation() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE updateColumnValueOperation(a INT)");
            stmt.execute("INSERT INTO updateColumnValueOperation VALUES (1), (2), (3)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("updateColumnValueOperation")
                            .setSchema(database)
                            .setUpdateColumnValue(UpdateColumnValueOperation.newBuilder()
                                    .setColumn("a")
                                    .setValue("4")
                                    .build()
                            )
                    )
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            checkResult("SELECT * FROM updateColumnValueOperation ORDER BY a", Arrays.asList(
                    Collections.singletonList("4"),
                    Collections.singletonList("4"),
                    Collections.singletonList("4")
            ));
        }
    }

    @Test
    public void liveToSoftDelete() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE liveToSoftDelete(a INT)");
            stmt.execute("INSERT INTO liveToSoftDelete VALUES (1), (2)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("liveToSoftDelete")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.LIVE_TO_SOFT_DELETE)
                                            .setSoftDeletedColumn("b")
                            ))
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = JDBCUtil.getTable(conf, database, "liveToSoftDelete", "liveToSoftDelete", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(1).getName());

            checkResult("SELECT a, b FROM liveToSoftDelete ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "0"),
                    Arrays.asList("2", "0")
            ));
        }
    }

    @Test
    public void liveToHistory() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE liveToHistory(a INT PRIMARY KEY)");
            stmt.execute("INSERT INTO liveToHistory VALUES (1), (2)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("liveToHistory")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.LIVE_TO_HISTORY)
                            ))
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = JDBCUtil.getTable(conf, database, "liveToHistory", "liveToHistory", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(3).getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(1).getType());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(2).getType());
            Assertions.assertEquals(DataType.BOOLEAN, columns.get(3).getType());
            Assertions.assertTrue(columns.get(1).getPrimaryKey());

            checkResult("SELECT a, _fivetran_end, _fivetran_active FROM liveToHistory ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "9999-12-31 23:59:59.999999", "1"),
                    Arrays.asList("2", "9999-12-31 23:59:59.999999", "1")
            ));
        }
    }

    @Test
    public void softDeleteToHistory() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE softDeleteToHistory(a INT PRIMARY KEY, _fivetran_synced DATETIME(6), _fivetran_deleted BOOL)");
            stmt.execute("INSERT INTO softDeleteToHistory VALUES (1, '2020-01-01 01:01:01', 0), (2, '2020-01-01 01:01:01', 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("softDeleteToHistory")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.SOFT_DELETE_TO_HISTORY)
                                            .setSoftDeletedColumn("_fivetran_deleted")
                            ))
                    .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = JDBCUtil.getTable(conf, database, "softDeleteToHistory", "softDeleteToHistory", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(3).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(4).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(5).getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(3).getType());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(4).getType());
            Assertions.assertEquals(DataType.BOOLEAN, columns.get(5).getType());
            Assertions.assertTrue(columns.get(3).getPrimaryKey());

            checkResult("SELECT a, _fivetran_start, _fivetran_end, _fivetran_active FROM softDeleteToHistory ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "2020-01-01 01:01:01.000000", "9999-12-31 23:59:59.999999", "1"),
                    Arrays.asList("2", "1000-01-01 00:00:00.000000", "1000-01-01 00:00:00.000000", "0")
            ));
        }
    }

    @Test
    public void softDeleteToLive() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE softDeleteToLive(a INT PRIMARY KEY, _fivetran_deleted BOOL)");
            stmt.execute("INSERT INTO softDeleteToLive VALUES (1, 0), (2, 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                .putAllConfiguration(confMap)
                .setDetails(MigrationDetails.newBuilder()
                    .setTable("softDeleteToLive")
                    .setSchema(database)
                    .setTableSyncModeMigration(
                        TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.SOFT_DELETE_TO_LIVE)
                            .setSoftDeletedColumn("_fivetran_deleted")
                    ))
                .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = JDBCUtil.getTable(conf, database, "softDeleteToLive", "softDeleteToLive", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            checkResult("SELECT a FROM softDeleteToLive ORDER BY a", Collections.singletonList(
                Collections.singletonList("1")
            ));
        }
    }
}
