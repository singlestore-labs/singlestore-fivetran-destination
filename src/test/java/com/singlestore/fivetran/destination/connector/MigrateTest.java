package com.singlestore.fivetran.destination.connector;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
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
}
