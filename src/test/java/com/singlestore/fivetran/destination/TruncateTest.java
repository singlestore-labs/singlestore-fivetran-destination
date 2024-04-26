package com.singlestore.fivetran.destination;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import com.google.protobuf.Timestamp;
import fivetran_sdk.Column;
import fivetran_sdk.CreateTableRequest;
import fivetran_sdk.DataType;
import fivetran_sdk.SoftTruncate;
import fivetran_sdk.Table;
import fivetran_sdk.TruncateRequest;

public class TruncateTest extends IntegrationTestBase {
    @Test
    public void softTruncate() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement();) {
            Table t = Table.newBuilder().setName("softTruncate")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.INT)
                                    .setPrimaryKey(false).build(),
                            Column.newBuilder().setName("_fivetran_synced")
                                    .setType(DataType.UTC_DATETIME).setPrimaryKey(false).build(),
                            Column.newBuilder().setName("_fivetran_deleted")
                                    .setType(DataType.BOOLEAN).setPrimaryKey(false).build()))
                    .build();

            CreateTableRequest cr =
                    CreateTableRequest.newBuilder().setSchemaName(database).setTable(t).build();
            stmt.execute(JDBCUtil.generateCreateTableQuery(conf, stmt, cr));

            stmt.execute(String.format("USE %s", database));
            stmt.execute("INSERT INTO softTruncate VALUES (1, '2038-01-19 03:14:07.123455', 0)");
            stmt.execute("INSERT INTO softTruncate VALUES (2, '2038-01-19 03:14:07.123456', 1)");
            stmt.execute("INSERT INTO softTruncate VALUES (3, '2038-01-19 03:14:07.123456', 0)");
            stmt.execute("INSERT INTO softTruncate VALUES (4, '2038-01-19 03:14:07.123457', 0)");
            stmt.execute("INSERT INTO softTruncate VALUES (5, '2038-01-19 03:14:07.123460', 0)");

            TruncateRequest tr = TruncateRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTableName("softTruncate")
                    .setSoft(SoftTruncate.newBuilder().setDeletedColumn("_fivetran_deleted"))
                    .setSyncedColumn("_fivetran_synced")
                    .setUtcDeleteBefore(
                            Timestamp.newBuilder().setSeconds(2147483647L).setNanos(123458000))
                    .build();

            stmt.execute(JDBCUtil.generateTruncateTableQuery(conf, tr));

            checkResult("SELECT * FROM `softTruncate` ORDER BY a",
                    Arrays.asList(Arrays.asList("1", "2038-01-19 03:14:07.123455", "1"),
                            Arrays.asList("2", "2038-01-19 03:14:07.123456", "1"),
                            Arrays.asList("3", "2038-01-19 03:14:07.123456", "1"),
                            Arrays.asList("4", "2038-01-19 03:14:07.123457", "1"),
                            Arrays.asList("5", "2038-01-19 03:14:07.123460", "0")));
        }
    }

    @Test
    public void hardTruncate() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement();) {
            Table t = Table.newBuilder().setName("hardTruncate")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.INT)
                                    .setPrimaryKey(false).build(),
                            Column.newBuilder().setName("_fivetran_synced")
                                    .setType(DataType.UTC_DATETIME).setPrimaryKey(false).build(),
                            Column.newBuilder().setName("_fivetran_deleted")
                                    .setType(DataType.BOOLEAN).setPrimaryKey(false).build()))
                    .build();

            CreateTableRequest cr =
                    CreateTableRequest.newBuilder().setSchemaName(database).setTable(t).build();
            stmt.execute(JDBCUtil.generateCreateTableQuery(conf, stmt, cr));

            stmt.execute(String.format("USE %s", database));
            stmt.execute("INSERT INTO hardTruncate VALUES (1, '2038-01-19 03:14:07.123455', 0)");
            stmt.execute("INSERT INTO hardTruncate VALUES (2, '2038-01-19 03:14:07.123456', 1)");
            stmt.execute("INSERT INTO hardTruncate VALUES (3, '2038-01-19 03:14:07.123456', 0)");
            stmt.execute("INSERT INTO hardTruncate VALUES (4, '2038-01-19 03:14:07.123457', 0)");
            stmt.execute("INSERT INTO hardTruncate VALUES (5, '2038-01-19 03:14:07.123460', 0)");

            TruncateRequest tr = TruncateRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTableName("hardTruncate")
                    .setSyncedColumn("_fivetran_synced")
                    .setUtcDeleteBefore(
                            Timestamp.newBuilder().setSeconds(2147483647L).setNanos(123458000))
                    .build();

            stmt.execute(JDBCUtil.generateTruncateTableQuery(conf, tr));

            checkResult("SELECT * FROM `hardTruncate` ORDER BY a",
                    Arrays.asList(Arrays.asList("5", "2038-01-19 03:14:07.123460", "0")));
        }
    }
}
