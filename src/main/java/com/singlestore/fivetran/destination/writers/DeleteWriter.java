package com.singlestore.fivetran.destination.writers;

import com.singlestore.fivetran.destination.JDBCUtil;
import fivetran_sdk.Column;
import fivetran_sdk.DataType;
import fivetran_sdk.Table;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteWriter extends Writer {

    // TODO: make batch size configurable
    final int BATCH_SIZE = 10000;

    List<Integer> pkIds;
    List<Column> pkColumns;
    List<List<String>> batch;

    public DeleteWriter(Connection conn, String database, Table table) {
        super(conn, database, table);
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

        StringBuilder query = new StringBuilder(String.format("DELETE FROM %s.%s WHERE ",
                JDBCUtil.escapeIdentifier(database),
                JDBCUtil.escapeIdentifier(table.getName())));

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
                    switch (pkColumns.get(j).getType()) {
                        case BOOLEAN:
                            stmt.setBoolean(paramIndex, Boolean.parseBoolean(value));
                        case SHORT:
                            stmt.setShort(paramIndex, Short.parseShort(value));
                        case INT:
                            stmt.setInt(paramIndex, Integer.parseInt(value));
                        case LONG:
                            stmt.setLong(paramIndex, Long.parseLong(value));
                        case FLOAT:
                            stmt.setFloat(paramIndex, Float.parseFloat(value));
                        case DOUBLE:
                            stmt.setDouble(paramIndex, Double.parseDouble(value));
                        case BINARY:
                            stmt.setBytes(paramIndex, value.getBytes());

                        case DECIMAL:
                        case NAIVE_DATE:
                        case NAIVE_DATETIME:
                        case UTC_DATETIME:
                        case XML:
                        case STRING:
                        case JSON:
                        case UNSPECIFIED:
                        default:
                            stmt.setString(paramIndex, value);
                    }
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
