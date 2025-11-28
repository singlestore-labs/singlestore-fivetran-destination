package com.singlestore.fivetran.destination.connector;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    public void historyToLive() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE historyToLive(a INT PRIMARY KEY, _fivetran_start DATETIME(6), _fivetran_end DATETIME(6), _fivetran_active BOOL)");
            stmt.execute("INSERT INTO historyToLive VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0), (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                .putAllConfiguration(confMap)
                .setDetails(MigrationDetails.newBuilder()
                    .setTable("historyToLive")
                    .setSchema(database)
                    .setTableSyncModeMigration(
                        TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.HISTORY_TO_LIVE)
                            .setKeepDeletedRows(true)
                    ))
                .build();

            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = JDBCUtil.getTable(conf, database, "historyToLive", "historyToLive", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            checkResult("SELECT a FROM historyToLive ORDER BY a", Arrays.asList(
                Collections.singletonList("1"),
                Collections.singletonList("2")
            ));

            stmt.execute("DROP TABLE historyToLive");
            stmt.execute("CREATE TABLE historyToLive(a INT PRIMARY KEY, _fivetran_start DATETIME(6), _fivetran_end DATETIME(6), _fivetran_active BOOL)");
            stmt.execute("INSERT INTO historyToLive VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0), (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)");

            request = MigrateRequest.newBuilder()
                .putAllConfiguration(confMap)
                .setDetails(MigrationDetails.newBuilder()
                    .setTable("historyToLive")
                    .setSchema(database)
                    .setTableSyncModeMigration(
                        TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.HISTORY_TO_LIVE)
                            .setKeepDeletedRows(false)
                    ))
                .build();

            queries = JDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            t = JDBCUtil.getTable(conf, database, "historyToLive", "historyToLive", testWarningHandle);
            columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            checkResult("SELECT a FROM historyToLive ORDER BY a", Collections.singletonList(
                Collections.singletonList("2")
            ));
        }
    }
}
