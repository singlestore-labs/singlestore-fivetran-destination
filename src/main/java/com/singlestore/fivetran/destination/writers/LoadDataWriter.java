package com.singlestore.fivetran.destination.writers;

import com.singlestore.fivetran.destination.JDBCUtil;
import fivetran_sdk.Table;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class LoadDataWriter extends Writer {

    final int BUFFER_SIZE = 524288;
    PipedOutputStream outputStream = new PipedOutputStream();
    PipedInputStream inputStream = new PipedInputStream(outputStream, BUFFER_SIZE);
    Thread t;
    final SQLException[] queryException = new SQLException[1];

    public LoadDataWriter(Statement stmt, String database, Table table) throws IOException {
        super(stmt, database, table);
    }

    @Override
    public void setHeader(List<String> header) throws SQLException {
        // TODO: add compression
        // TODO: handle NULL
        String query = String.format("LOAD DATA LOCAL INFILE '###.tsv' REPLACE INTO TABLE %s.%s (%s)",
                JDBCUtil.escapeIdentifier(database),
                JDBCUtil.escapeIdentifier(table.getName()),
                header.stream()
                        .map(JDBCUtil::escapeIdentifier)
                        .collect(Collectors.joining(", "))
                );

        ((com.singlestore.jdbc.Statement)stmt).setNextLocalInfileInputStream(inputStream);

        t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    stmt.executeUpdate(query);
                } catch (SQLException e) {
                    queryException[0] = e;
                }
            }
        });
        t.start();
    }

    @Override
    public void writeRow(List<String> row) throws IOException {
        for (int i = 0; i < row.size(); i++) {
            String value = row.get(i);
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
    }

    @Override
    public void commit() throws InterruptedException, IOException {
        outputStream.close();
        t.join();
    }

    @Override
    public void abort(Exception writerException) throws Exception {
        try {
            outputStream.close();
        } catch (Exception ignored) {}

        try {
            stmt.cancel();
        } catch (Exception ignored) {}

        try {
            t.join();
        } catch (Exception ignored) {}

        if (writerException instanceof IOException && writerException.getMessage().contains("Pipe closed")) {
            // The actual exception occured in the query thread
            throw queryException[0];
        } else {
            throw writerException;
        }
    }
}
