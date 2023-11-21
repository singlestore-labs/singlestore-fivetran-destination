package com.singlestore.fivetran.destination.writers;

import com.singlestore.fivetran.destination.JDBCUtil;
import fivetran_sdk.Column;
import fivetran_sdk.CsvFileParams;
import fivetran_sdk.Table;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteWriter extends Writer {

    // TODO: PLAT-6897 make batch size configurable
    final int BATCH_SIZE = 10000;

    List<Integer> pkIds = new ArrayList<>();
    List<Column> pkColumns = new ArrayList<>();
    List<List<String>> batch = new ArrayList<>();

    public DeleteWriter(Connection conn, String database, Table table, CsvFileParams params) {
        super(conn, database, table, params);
    }

    @Override
    public void setHeader(List<String> header) {
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : table.getColumnsList()) {
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
        batch.add(row);
        if (batch.size() == BATCH_SIZE) {
            processBatch();
        }
    }

    private void processBatch() throws SQLException {
        if (batch.isEmpty()) {
            return;
        }

        StringBuilder query = new StringBuilder(String.format("DELETE FROM %s WHERE ",
                JDBCUtil.escapeTable(database, table.getName())));

        String condition = pkColumns.stream()
                .map(column -> String.format("%s = ?",
                        JDBCUtil.escapeIdentifier(column.getName())))
                .collect(Collectors.joining(" AND "));


        for (int i = 0; i < batch.size(); i++) {
            query.append("(");
            query.append(condition);
            query.append(")");

            if (i != batch.size() - 1) {
                query.append(" OR ");
            }
        }

        try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
            for (int i = 0; i < batch.size(); i++) {
                List<String> row = batch.get(i);
                for (int j = 0; j < pkIds.size(); j++) {
                    int paramIndex = i*pkIds.size() + j + 1;
                    String value = row.get(pkIds.get(j));
                    JDBCUtil.setParameter(stmt, paramIndex, pkColumns.get(j).getType(), value, params.getNullString());
                }
            }

            stmt.execute();
        }

        batch.clear();
    }

    @Override
    public void commit() throws SQLException {
        processBatch();
    }
}
