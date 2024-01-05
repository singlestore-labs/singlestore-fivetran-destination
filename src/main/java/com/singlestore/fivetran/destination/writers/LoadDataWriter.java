package com.singlestore.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.singlestore.fivetran.destination.JDBCUtil;

import fivetran_sdk.Column;
import fivetran_sdk.CsvFileParams;
import fivetran_sdk.DataType;
import fivetran_sdk.Table;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// TODO: PLAT-6897 allow to configure batch size in writers
public class LoadDataWriter extends Writer {

    final int BUFFER_SIZE = 524288;

    List<Column> columns = new ArrayList<>();
    PipedOutputStream outputStream = new PipedOutputStream();
    PipedInputStream inputStream = new PipedInputStream(outputStream, BUFFER_SIZE);
    Thread t;
    final SQLException[] queryException = new SQLException[1];
    Statement stmt;

    public LoadDataWriter(Connection conn, String database, Table table, CsvFileParams params, Map<String, ByteString> secretKeys) throws IOException {
        super(conn, database, table, params, secretKeys);
    }

    @Override
    public void setHeader(List<String> header) throws SQLException {
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : table.getColumnsList()) {
            nameToColumn.put(column.getName(), column);
        }

        for (String name : header) {
            columns.add(nameToColumn.get(name));
        }

        // TODO: PLAT-6898 add compression
        String query = String.format("LOAD DATA LOCAL INFILE '###.tsv' REPLACE INTO TABLE %s (%s) NULL DEFINED BY %s",
                JDBCUtil.escapeTable(database, table.getName()),
                header.stream()
                        .map(JDBCUtil::escapeIdentifier)
                        .collect(Collectors.joining(", ")),
                JDBCUtil.escapeString(params.getNullString())
                );

        stmt = conn.createStatement();
        ((com.singlestore.jdbc.Statement)stmt).setNextLocalInfileInputStream(inputStream);

        t = new Thread(() -> {
            try {
                stmt.executeUpdate(query);
                stmt.close();
            } catch (SQLException e) {
                queryException[0] = e;
            }
        });
        t.start();
    }

    @Override
    public void writeRow(List<String> row) throws Exception {
        try {
            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);

                if (columns.get(i).getType() == DataType.BOOLEAN) {
                    if (row.get(i).equalsIgnoreCase("true")) {
                        value = "1";
                    } else if (row.get(i).equalsIgnoreCase("false")) {
                        value = "0";
                    }
                }

                if (value.indexOf('\\') != -1) {
                    value = value.replace("\\", "\\\\");
                }
                if (value.indexOf('\n') != -1) {
                    value = value.replace("\n", "\\n");
                }
                if (value.indexOf('\t') != -1) {
                    value = value.replace("\t", "\\t");
                }

                outputStream.write(value.getBytes());

                if (i != row.size() - 1) {
                    outputStream.write('\t');
                } else {
                    outputStream.write('\n');
                }
            }
        } catch (Exception e) {
            abort(e);
        }
    }

    @Override
    public void commit() throws InterruptedException, IOException, SQLException {
        outputStream.close();
        t.join();

        if (queryException[0] != null) {
            throw queryException[0];
        }
    }

    private void abort(Exception writerException) throws Exception {
        try {
            outputStream.close();
        } catch (Exception ignored) {}

        try {
            stmt.cancel();
        } catch (Exception ignored) {}

        try {
            t.interrupt();
        } catch (Exception ignored) {}

        if (writerException instanceof IOException && writerException.getMessage().contains("Pipe closed")) {
            // The actual exception occurred in the query thread
            throw queryException[0];
        } else {
            throw writerException;
        }
    }
}
