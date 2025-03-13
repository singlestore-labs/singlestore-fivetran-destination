package com.singlestore.fivetran.destination.connector.writers;

import com.google.protobuf.ByteString;
import com.singlestore.fivetran.destination.connector.JDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EarliestStartHistoryWriter extends Writer {
    private static final Logger logger = LoggerFactory.getLogger(EarliestStartHistoryWriter.class);

    public EarliestStartHistoryWriter(Connection conn, String database, String table, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
    }

    List<Column> headerColumns = new ArrayList<>();
    Integer earliestFivetranStartPos;

    @Override
    public void setHeader(List<String> header) throws SQLException, IOException {
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            nameToColumn.put(column.getName(), column);
        }

        for (int i = 0; i < header.size(); i++) {
            String name = header.get(i);
            if (name.equals("_fivetran_start")) {
                earliestFivetranStartPos = i;
            }
            headerColumns.add(nameToColumn.get(name));
        }

        if (earliestFivetranStartPos == null) {
            throw new IllegalArgumentException("File doesn't contain _fivetran_start column");
        }
    }

    public void writeDelete(List<String> row) throws Exception {
        StringBuilder deleteQuery = new StringBuilder(String.format("DELETE FROM %s WHERE ", JDBCUtil.escapeTable(database, table)));

        boolean firstPKColumn = true;
        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                if (firstPKColumn) {
                    deleteQuery.append(
                            String.format("%s = ? ", JDBCUtil.escapeIdentifier(c.getName())));
                    firstPKColumn = false;
                } else {
                    deleteQuery.append(
                            String.format("AND %s = ? ", JDBCUtil.escapeIdentifier(c.getName())));
                }
            }
        }

        deleteQuery.append("AND `_fivetran_start` >= ?");

        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(deleteQuery.toString())) {
            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                Column c = headerColumns.get(i);
                if (c == null || !c.getPrimaryKey() || c.getName().equals("_fivetran_start")) {
                    continue;
                }

                paramIndex++;
                JDBCUtil.setParameter(stmt, paramIndex, c.getType(), value, params.getNullString());
            }

            paramIndex++;
            JDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(earliestFivetranStartPos), params.getNullString());

            stmt.execute();
        }
    }

    public void writeUpdate(List<String> row) throws Exception {
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET `_fivetran_active` = FALSE, `_fivetran_end` = DATE_SUB(?,  INTERVAL 1 MICROSECOND) WHERE `_fivetran_active` = TRUE ",
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
            JDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(earliestFivetranStartPos), params.getNullString());

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
    public void writeRow(List<String> row) throws Exception {
        writeDelete(row);
        writeUpdate(row);
    }

    @Override
    public void commit() throws InterruptedException, IOException, SQLException {

    }
}
