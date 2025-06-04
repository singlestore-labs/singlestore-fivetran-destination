package com.singlestore.fivetran.destination.connector;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.singlestore.fivetran.destination.connector.writers.DeleteWriter;
import com.singlestore.fivetran.destination.connector.writers.UpdateWriter;
import com.singlestore.fivetran.destination.connector.writers.LoadDataWriter;
import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SingleDatabaseTest extends IntegrationTestBase {
    static ImmutableMap<String, String> confMap =
            ImmutableMap.of("host", host, "port", port, "user", user, "password", password, "database", database);
    static SingleStoreConfiguration conf = new SingleStoreConfiguration(confMap);

    @Test
    public void checkSingleDatabase() throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", database));

            // CREATE TABLE
            Table t = Table.newBuilder().setName("checkSingleDatabase").addAllColumns(Arrays.asList(
                    Column.newBuilder().setName("a").setType(DataType.INT).setPrimaryKey(true)
                            .build())).build();
            CreateTableRequest createRequest =
                    CreateTableRequest.newBuilder().setSchemaName("schema").setTable(t).build();

            String query = JDBCUtil.generateCreateTableQuery(conf, stmt, createRequest);
            stmt.execute(query);

            // GET TABLE
            Table result = JDBCUtil.getTable(conf, database, "schema__checkSingleDatabase", "checkSingleDatabase", testWarningHandle);
            assertEquals("checkSingleDatabase", result.getName());
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            // ALTER TABLE
            t = Table.newBuilder().setName("checkSingleDatabase").addAllColumns(Arrays.asList(
                    Column.newBuilder().setName("a").setType(DataType.INT).setPrimaryKey(true)
                            .build(),
                    Column.newBuilder().setName("b").setType(DataType.INT).setPrimaryKey(false)
                            .build(),
                    Column.newBuilder().setName("c")
                            .setType(DataType.UTC_DATETIME).setPrimaryKey(false).build())
            ).build();
            AlterTableRequest alterRequest = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName("schema").setTable(t).build();
            List<JDBCUtil.QueryWithCleanup> queries = JDBCUtil.generateAlterTableQuery(alterRequest, testWarningHandle);
            for (JDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            result = JDBCUtil.getTable(conf, database, "schema__checkSingleDatabase", "checkSingleDatabase", testWarningHandle);
            assertEquals("checkSingleDatabase", result.getName());
            columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());
            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.INT, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());
            assertEquals("c", columns.get(2).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(2).getType());
            assertEquals(false, columns.get(2).getPrimaryKey());

            // WRITE DATA
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w = new LoadDataWriter(conn, database, "schema__checkSingleDatabase", t.getColumnsList(), params, null, 123, testWarningHandle);
            w.setHeader(List.of("a", "b", "c"));
            w.writeRow(List.of("1", "2", "2038-01-19 03:14:07.123455"));
            w.writeRow(List.of("3", "4", "2038-01-19 03:14:07.123460"));
            w.commit();
            checkResult("SELECT * FROM `schema__checkSingleDatabase` ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "2", "2038-01-19 03:14:07.123455"),
                    Arrays.asList("3", "4", "2038-01-19 03:14:07.123460")));

            // UPDATE DATA
            UpdateWriter u = new UpdateWriter(conn, database, "schema__checkSingleDatabase", t.getColumnsList(), params, null, 123);
            u.setHeader(List.of("a", "b", "c"));
            u.writeRow(List.of("1", "5", "2038-01-19 03:14:07.123455"));
            u.commit();
            checkResult("SELECT * FROM `schema__checkSingleDatabase` ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "5", "2038-01-19 03:14:07.123455"),
                    Arrays.asList("3", "4", "2038-01-19 03:14:07.123460")));

            // DELETE DATA
            DeleteWriter d = new DeleteWriter(conn, database, "schema__checkSingleDatabase", t.getColumnsList(), params, null, 123);
            d.setHeader(List.of("a", "b", "c"));
            d.writeRow(List.of("3", "4", "2038-01-19 03:14:07.123460"));
            d.commit();
            checkResult("SELECT * FROM `schema__checkSingleDatabase` ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "5", "2038-01-19 03:14:07.123455")));

            // TRUNCATE
            TruncateRequest tr = TruncateRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName("schema").setTableName("checkSingleDatabase")
                    .setSyncedColumn("c")
                    .setUtcDeleteBefore(
                            Timestamp.newBuilder().setSeconds(2147483647L).setNanos(123458000))
                    .build();

            stmt.execute(JDBCUtil.generateTruncateTableQuery(conf, tr));

            checkResult("SELECT * FROM `schema__checkSingleDatabase` ORDER BY a", Arrays.asList());
        }
    }
}
