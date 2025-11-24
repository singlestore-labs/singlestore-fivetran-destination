package com.singlestore.fivetran.destination.connector;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
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
}
