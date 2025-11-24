package com.singlestore.fivetran.destination.connector;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
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
}
