package com.singlestore.fivetran.destination.connector.writers;

import com.google.protobuf.ByteString;
import com.singlestore.fivetran.destination.connector.JDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteHistoryWriter extends Writer {

    public DeleteHistoryWriter(Connection conn, String database, String table, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
    }

    List<Column> headerColumns = new ArrayList<>();
    Integer fivetranEndPos;

    @Override
    public void setHeader(List<String> header) throws SQLException, IOException {
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            nameToColumn.put(column.getName(), column);
        }

        for (int i = 0; i < header.size(); i++) {
            String name = header.get(i);
            if (name.equals("_fivetran_end")) {
                fivetranEndPos = i;
            }
            headerColumns.add(nameToColumn.get(name));
        }

        if (fivetranEndPos == null) {
            throw new IllegalArgumentException("File doesn't contain _fivetran_end column");
        }
    }

    @Override
    public void writeRow(List<String> row) throws Exception {
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET `_fivetran_active` = FALSE, `_fivetran_end` = ? WHERE `_fivetran_active` = TRUE ",
                JDBCUtil.escapeTable(database, table)));

        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                updateQuery.append(
                        String.format("AND %s = ? ", JDBCUtil.escapeIdentifier(c.getName())));
            }
        }

        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(updateQuery.toString())) {
            paramIndex++;
            JDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(fivetranEndPos), params.getNullString());

            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                Column c = headerColumns.get(i);
                if (c == null || !c.getPrimaryKey() || c.getName().equals("_fivetran_start")) {
                    continue;
                }

                paramIndex++;
                JDBCUtil.setParameter(stmt, paramIndex, c.getType(), value, params.getNullString());
            }

            stmt.execute();
        }
    }

    @Override
    public void commit() {
    }
}
