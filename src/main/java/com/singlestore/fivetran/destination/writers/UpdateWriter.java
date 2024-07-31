package com.singlestore.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.singlestore.fivetran.destination.JDBCUtil;
import fivetran_sdk.Column;
import fivetran_sdk.CsvFileParams;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateWriter extends Writer {
    public UpdateWriter(Connection conn, String database, String table, List<Column> columns,
            CsvFileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
    }

    List<Column> headerColumns = new ArrayList<>();

    @Override
    public void setHeader(List<String> header) {
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            nameToColumn.put(column.getName(), column);
        }

        for (String name : header) {
            headerColumns.add(nameToColumn.get(name));
        }
    }

    @Override
    public void writeRow(List<String> row) throws SQLException {
        StringBuilder updateClause = new StringBuilder(
                String.format("UPDATE %s SET ", JDBCUtil.escapeTable(database, table)));
        StringBuilder whereClause = new StringBuilder("WHERE ");

        boolean firstUpdateColumn = true;
        boolean firstPKColumn = true;

        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (!row.get(i).equals(params.getUnmodifiedString())) {
                if (firstUpdateColumn) {
                    updateClause.append(
                            String.format("%s = ?", JDBCUtil.escapeIdentifier(c.getName())));
                    firstUpdateColumn = false;
                } else {
                    updateClause.append(
                            String.format(", %s = ?", JDBCUtil.escapeIdentifier(c.getName())));
                }
            }

            if (c.getPrimaryKey()) {
                if (firstPKColumn) {
                    whereClause.append(
                            String.format("%s = ?", JDBCUtil.escapeIdentifier(c.getName())));
                    firstPKColumn = false;
                } else {
                    whereClause.append(
                            String.format(", %s = ?", JDBCUtil.escapeIdentifier(c.getName())));
                }
            }
        }

        if (firstUpdateColumn) {
            // No column is updated
            return;
        }

        String query = updateClause.toString() + " " + whereClause;

        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                if (value.equals(params.getUnmodifiedString())) {
                    continue;
                }

                paramIndex++;
                JDBCUtil.setParameter(stmt, paramIndex, headerColumns.get(i).getType(), value,
                        params.getNullString());
            }

            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                if (!headerColumns.get(i).getPrimaryKey()) {
                    continue;
                }

                paramIndex++;
                JDBCUtil.setParameter(stmt, paramIndex, headerColumns.get(i).getType(), value,
                        params.getNullString());
            }

            stmt.execute();
        }
    }

    @Override
    public void commit() {}
}
