package com.singlestore.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import fivetran_sdk.Column;
import fivetran_sdk.CreateTableRequest;
import fivetran_sdk.DataType;
import fivetran_sdk.Table;

public class CreateTableTest extends IntegrationTestBase {
    @Test
    public void allDataTypes() throws SQLException, Exception {
        Table allTypesCreateTable = Table.newBuilder()
            .setName("allTypesCreateTable")
            .addAllColumns(Arrays.asList(
                Column.newBuilder()
                    .setName("boolean")
                    .setType(DataType.BOOLEAN)
                    .setPrimaryKey(true)
                    .build(),
                Column.newBuilder()
                    .setName("short")
                    .setType(DataType.SHORT)
                    .setPrimaryKey(true)
                    .build(),
                Column.newBuilder()
                    .setName("int")
                    .setType(DataType.INT)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("long")
                    .setType(DataType.LONG)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("float")
                    .setType(DataType.FLOAT)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("double")
                    .setType(DataType.DOUBLE)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("decimal")
                    .setType(DataType.DECIMAL)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("binary")
                    .setType(DataType.BINARY)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("json")
                    .setType(DataType.JSON)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("naive_date")
                    .setType(DataType.NAIVE_DATE)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("naive_datetime")
                    .setType(DataType.NAIVE_DATETIME)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("utc_datetime")
                    .setType(DataType.UTC_DATETIME)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("string")
                    .setType(DataType.STRING)
                    .setPrimaryKey(false)
                    .build(),
                    Column.newBuilder()
                    .setName("xml")
                    .setType(DataType.XML)
                    .setPrimaryKey(false)
                    .build(),
                Column.newBuilder()
                    .setName("unspecified")
                    .setType(DataType.UNSPECIFIED)
                    .setPrimaryKey(false)
                    .build()
            )).build();
        
        CreateTableRequest request = CreateTableRequest.newBuilder()
            .setSchemaName(database)
            .setTable(allTypesCreateTable)
            .build();

        String query = JDBCUtil.generateCreateTableQuery(request);
        try (Connection conn = JDBCUtil.createConnection(conf);
            Statement stmt = conn.createStatement();
        ) {
            stmt.execute(query);
            Table result = JDBCUtil.getTable(conf, database, "allTypesCreateTable");
            assertEquals("allTypesCreateTable", result.getName());
            List<Column> columns = result.getColumnsList();

            assertEquals("boolean", columns.get(0).getName());
            assertEquals(DataType.BOOLEAN, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            assertEquals("short", columns.get(1).getName());
            assertEquals(DataType.SHORT, columns.get(1).getType());
            assertEquals(true, columns.get(1).getPrimaryKey());

            assertEquals("int", columns.get(2).getName());
            assertEquals(DataType.INT, columns.get(2).getType());
            assertEquals(false, columns.get(2).getPrimaryKey());

            assertEquals("long", columns.get(3).getName());
            assertEquals(DataType.LONG, columns.get(3).getType());
            assertEquals(false, columns.get(3).getPrimaryKey());

            assertEquals("float", columns.get(4).getName());
            assertEquals(DataType.FLOAT, columns.get(4).getType());
            assertEquals(false, columns.get(4).getPrimaryKey());

            assertEquals("double", columns.get(5).getName());
            assertEquals(DataType.DOUBLE, columns.get(5).getType());
            assertEquals(false, columns.get(5).getPrimaryKey());

            assertEquals("decimal", columns.get(6).getName());
            assertEquals(DataType.DECIMAL, columns.get(6).getType());
            assertEquals(false, columns.get(6).getPrimaryKey());

            assertEquals("binary", columns.get(7).getName());
            assertEquals(DataType.BINARY, columns.get(7).getType());
            assertEquals(false, columns.get(7).getPrimaryKey());

            assertEquals("json", columns.get(8).getName());
            assertEquals(DataType.JSON, columns.get(8).getType());
            assertEquals(false, columns.get(8).getPrimaryKey());

            assertEquals("naive_date", columns.get(9).getName());
            assertEquals(DataType.NAIVE_DATE, columns.get(9).getType());
            assertEquals(false, columns.get(9).getPrimaryKey());

            assertEquals("naive_datetime", columns.get(10).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(10).getType());
            assertEquals(false, columns.get(10).getPrimaryKey());

            assertEquals("utc_datetime", columns.get(11).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(11).getType());
            assertEquals(false, columns.get(11).getPrimaryKey());

            assertEquals("string", columns.get(12).getName());
            assertEquals(DataType.STRING, columns.get(12).getType());
            assertEquals(false, columns.get(12).getPrimaryKey());

            assertEquals("xml", columns.get(13).getName());
            assertEquals(DataType.STRING, columns.get(13).getType());
            assertEquals(false, columns.get(13).getPrimaryKey());

            assertEquals("unspecified", columns.get(14).getName());
            assertEquals(DataType.STRING, columns.get(14).getType());
            assertEquals(false, columns.get(14).getPrimaryKey());
        }
    }
}
