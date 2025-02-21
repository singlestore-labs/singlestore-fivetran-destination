package com.singlestore.fivetran.destination.connector.writers;

import com.google.protobuf.ByteString;
import com.singlestore.fivetran.destination.connector.JDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteWriter extends Writer {
    List<Integer> pkIds = new ArrayList<>();
    List<Column> pkColumns = new ArrayList<>();
    List<List<String>> rows = new ArrayList<>();

    public DeleteWriter(Connection conn, String database, String table, List<Column> columns,
            FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
    }

    @Override
    public void setHeader(List<String> header) {
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            if (column.getPrimaryKey()) {
                nameToColumn.put(column.getName(), column);
            }
        }

        for (int i = 0; i < header.size(); i++) {
            String columnName = header.get(i);
            if (nameToColumn.containsKey(columnName)) {
                pkIds.add(i);
                pkColumns.add(nameToColumn.get(columnName));
            }
        }
    }

    @Override
    public void writeRow(List<String> row) throws SQLException {
        rows.add(row);
    }

    @Override
    public void commit() throws SQLException {
        if (rows.isEmpty()) {
            return;
        }

        StringBuilder query = new StringBuilder(
                String.format("DELETE FROM %s WHERE ", JDBCUtil.escapeTable(database, table)));

        String condition = pkColumns.stream()
                .map(column -> String.format("%s = ?", JDBCUtil.escapeIdentifier(column.getName())))
                .collect(Collectors.joining(" AND "));

        for (int i = 0; i < rows.size(); i++) {
            query.append("(");
            query.append(condition);
            query.append(")");

            if (i != rows.size() - 1) {
                query.append(" OR ");
            }
        }

        try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
            for (int i = 0; i < rows.size(); i++) {
                List<String> row = rows.get(i);
                for (int j = 0; j < pkIds.size(); j++) {
                    int paramIndex = i * pkIds.size() + j + 1;
                    String value = row.get(pkIds.get(j));
                    JDBCUtil.setParameter(stmt, paramIndex, pkColumns.get(j).getType(), value,
                            params.getNullString());
                }
            }

            stmt.execute();
        }

        rows.clear();
    }
}
