package com.singlestore.fivetran.destination.writers;

import com.singlestore.fivetran.destination.JDBCUtil;
import fivetran_sdk.Column;
import fivetran_sdk.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateWriter extends Writer {
    public UpdateWriter(Connection conn, String database, Table table) {
        super(conn, database ,table);
    }

    List<Column> columns;

    @Override
    public void setHeader(List<String> header) {
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : table.getColumnsList()) {
            nameToColumn.put(column.getName(), column);
        }

        for (String name : header) {
            columns.add(nameToColumn.get(name));
        }
    }

    @Override
    public void writeRow(List<String> row) throws SQLException {
        StringBuilder updateClause = new StringBuilder(
                String.format("UPDATE %s.%s SET ",
                        JDBCUtil.escapeIdentifier(database),
                        JDBCUtil.escapeIdentifier(table.getName()))
        );
        StringBuilder whereClause = new StringBuilder("WHERE ");

        boolean firstUpdateColumn = true;
        boolean firstPKColumn = true;

        for (int i = 0; i < row.size(); i++) {
            Column c = columns.get(i);
            // TODO: handle no update
            if (true) {
                if (firstUpdateColumn) {
                    updateClause.append(String.format("%s = ?", JDBCUtil.escapeIdentifier(c.getName())));
                    firstUpdateColumn = false;
                } else {
                    updateClause.append(String.format(", %s = ?", JDBCUtil.escapeIdentifier(c.getName())));
                }
            }

            if (c.getPrimaryKey()) {
                if (firstPKColumn) {
                    whereClause.append(String.format("%s = ?", JDBCUtil.escapeIdentifier(c.getName())));
                    firstPKColumn = false;
                } else {
                    whereClause.append(String.format(", %s = ?", JDBCUtil.escapeIdentifier(c.getName())));
                }
            }
        }

        String query = updateClause.toString() + whereClause;

        try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                switch (columns.get(i).getType()) {
                    case BOOLEAN:
                        stmt.setBoolean(i + 1, Boolean.parseBoolean(value));
                    case SHORT:
                        stmt.setShort(i + 1, Short.parseShort(value));
                    case INT:
                        stmt.setInt(i + 1, Integer.parseInt(value));
                    case LONG:
                        stmt.setLong(i + 1, Long.parseLong(value));
                    case FLOAT:
                        stmt.setFloat(i + 1, Float.parseFloat(value));
                    case DOUBLE:
                        stmt.setDouble(i + 1, Double.parseDouble(value));
                    case BINARY:
                        stmt.setBytes(i + 1, value.getBytes());

                    case DECIMAL:
                    case NAIVE_DATE:
                    case NAIVE_DATETIME:
                    case UTC_DATETIME:
                    case XML:
                    case STRING:
                    case JSON:
                    case UNSPECIFIED:
                    default:
                        stmt.setString(i + 1, value);
                }
            }

            stmt.execute();
        }
    }

    @Override
    public void commit() {}
}
