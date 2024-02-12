package com.singlestore.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.Test;

import fivetran_sdk.Column;
import fivetran_sdk.DataType;
import fivetran_sdk.Table;

public class DescribeTableTest extends IntegrationTestBase {
    @Test
    public void allTypes() throws Exception {
        createAllTypesTable();

        try (Connection conn = JDBCUtil.createConnection(conf)) {
            Table allTypesTable = JDBCUtil.getTable(conf, database, "allTypesTable");
            assertEquals("allTypesTable", allTypesTable.getName());
            List<Column> columns = allTypesTable.getColumnsList();

            assertEquals("id", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            assertEquals("boolColumn", columns.get(1).getName());
            assertEquals(DataType.BOOLEAN, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());

            assertEquals("booleanColumn", columns.get(2).getName());
            assertEquals(DataType.BOOLEAN, columns.get(2).getType());
            assertEquals(false, columns.get(2).getPrimaryKey());

            assertEquals("bitColumn", columns.get(3).getName());
            assertEquals(DataType.BINARY, columns.get(3).getType());
            assertEquals(false, columns.get(3).getPrimaryKey());

            assertEquals("tinyintColumn", columns.get(4).getName());
            assertEquals(DataType.BOOLEAN, columns.get(4).getType());
            assertEquals(false, columns.get(4).getPrimaryKey());

            assertEquals("smallintColumn", columns.get(5).getName());
            assertEquals(DataType.SHORT, columns.get(5).getType());
            assertEquals(false, columns.get(5).getPrimaryKey());

            assertEquals("mediumintColumn", columns.get(6).getName());
            assertEquals(DataType.INT, columns.get(6).getType());
            assertEquals(false, columns.get(6).getPrimaryKey());

            assertEquals("intColumn", columns.get(7).getName());
            assertEquals(DataType.INT, columns.get(7).getType());
            assertEquals(false, columns.get(7).getPrimaryKey());

            assertEquals("integerColumn", columns.get(8).getName());
            assertEquals(DataType.INT, columns.get(8).getType());
            assertEquals(false, columns.get(8).getPrimaryKey());

            assertEquals("bigintColumn", columns.get(9).getName());
            assertEquals(DataType.LONG, columns.get(9).getType());
            assertEquals(false, columns.get(9).getPrimaryKey());

            assertEquals("floatColumn", columns.get(10).getName());
            assertEquals(DataType.FLOAT, columns.get(10).getType());
            assertEquals(false, columns.get(10).getPrimaryKey());

            assertEquals("doubleColumn", columns.get(11).getName());
            assertEquals(DataType.DOUBLE, columns.get(11).getType());
            assertEquals(false, columns.get(11).getPrimaryKey());

            assertEquals("realColumn", columns.get(12).getName());
            assertEquals(DataType.DOUBLE, columns.get(12).getType());
            assertEquals(false, columns.get(12).getPrimaryKey());

            assertEquals("dateColumn", columns.get(13).getName());
            assertEquals(DataType.NAIVE_DATE, columns.get(13).getType());
            assertEquals(false, columns.get(13).getPrimaryKey());

            assertEquals("timeColumn", columns.get(14).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(14).getType());
            assertEquals(false, columns.get(14).getPrimaryKey());

            assertEquals("time6Column", columns.get(15).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(15).getType());
            assertEquals(false, columns.get(15).getPrimaryKey());

            assertEquals("datetimeColumn", columns.get(16).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(16).getType());
            assertEquals(false, columns.get(16).getPrimaryKey());

            assertEquals("datetime6Column", columns.get(17).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(17).getType());
            assertEquals(false, columns.get(17).getPrimaryKey());

            assertEquals("timestampColumn", columns.get(18).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(18).getType());
            assertEquals(false, columns.get(18).getPrimaryKey());

            assertEquals("timestamp6Column", columns.get(19).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(19).getType());
            assertEquals(false, columns.get(19).getPrimaryKey());

            assertEquals("yearColumn", columns.get(20).getName());
            assertEquals(DataType.NAIVE_DATE, columns.get(20).getType());
            assertEquals(false, columns.get(20).getPrimaryKey());

            assertEquals("decimalColumn", columns.get(21).getName());
            assertEquals(DataType.DECIMAL, columns.get(21).getType());
            assertEquals(false, columns.get(21).getPrimaryKey());

            assertEquals("decColumn", columns.get(22).getName());
            assertEquals(DataType.DECIMAL, columns.get(22).getType());
            assertEquals(false, columns.get(22).getPrimaryKey());

            assertEquals("fixedColumn", columns.get(23).getName());
            assertEquals(DataType.DECIMAL, columns.get(23).getType());
            assertEquals(false, columns.get(23).getPrimaryKey());

            assertEquals("numericColumn", columns.get(24).getName());
            assertEquals(DataType.DECIMAL, columns.get(24).getType());
            assertEquals(false, columns.get(24).getPrimaryKey());

            assertEquals("charColumn", columns.get(25).getName());
            assertEquals(DataType.STRING, columns.get(25).getType());
            assertEquals(false, columns.get(25).getPrimaryKey());

            assertEquals("mediumtextColumn", columns.get(26).getName());
            assertEquals(DataType.STRING, columns.get(26).getType());
            assertEquals(false, columns.get(26).getPrimaryKey());

            assertEquals("binaryColumn", columns.get(27).getName());
            assertEquals(DataType.BINARY, columns.get(27).getType());
            assertEquals(false, columns.get(27).getPrimaryKey());

            assertEquals("varcharColumn", columns.get(28).getName());
            assertEquals(DataType.STRING, columns.get(28).getType());
            assertEquals(false, columns.get(28).getPrimaryKey());

            assertEquals("varbinaryColumn", columns.get(29).getName());
            assertEquals(DataType.BINARY, columns.get(29).getType());
            assertEquals(false, columns.get(29).getPrimaryKey());

            assertEquals("longtextColumn", columns.get(30).getName());
            assertEquals(DataType.STRING, columns.get(30).getType());
            assertEquals(false, columns.get(30).getPrimaryKey());

            assertEquals("textColumn", columns.get(31).getName());
            assertEquals(DataType.STRING, columns.get(31).getType());
            assertEquals(false, columns.get(31).getPrimaryKey());

            assertEquals("tinytextColumn", columns.get(32).getName());
            assertEquals(DataType.STRING, columns.get(32).getType());
            assertEquals(false, columns.get(32).getPrimaryKey());

            assertEquals("longblobColumn", columns.get(33).getName());
            assertEquals(DataType.BINARY, columns.get(33).getType());
            assertEquals(false, columns.get(33).getPrimaryKey());

            assertEquals("mediumblobColumn", columns.get(34).getName());
            assertEquals(DataType.BINARY, columns.get(34).getType());
            assertEquals(false, columns.get(34).getPrimaryKey());

            assertEquals("blobColumn", columns.get(35).getName());
            assertEquals(DataType.BINARY, columns.get(35).getType());
            assertEquals(false, columns.get(35).getPrimaryKey());

            assertEquals("tinyblobColumn", columns.get(36).getName());
            assertEquals(DataType.BINARY, columns.get(36).getType());
            assertEquals(false, columns.get(36).getPrimaryKey());

            assertEquals("jsonColumn", columns.get(37).getName());
            assertEquals(DataType.JSON, columns.get(37).getType());
            assertEquals(false, columns.get(37).getPrimaryKey());

            assertEquals("geographyColumn", columns.get(38).getName());
            assertEquals(DataType.STRING, columns.get(38).getType());
            assertEquals(false, columns.get(38).getPrimaryKey());

            assertEquals("geographypointColumn", columns.get(39).getName());
            assertEquals(DataType.STRING, columns.get(39).getType());
            assertEquals(false, columns.get(39).getPrimaryKey());
        }
    }
}
