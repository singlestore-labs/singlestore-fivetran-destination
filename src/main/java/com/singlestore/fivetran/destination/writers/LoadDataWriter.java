package com.singlestore.fivetran.destination.writers;

import fivetran_sdk.Table;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class LoadDataWriter extends Writer {

    PipedOutputStream outputStream = new PipedOutputStream();
    PipedInputStream inputStream;
    Thread t;
    final SQLException[] queryException = new SQLException[1];

    public LoadDataWriter(Statement stmt, String database, Table table) {
        super(stmt, database, table);
    }

    @Override
    public void setHeader(List<String> header) {
        String query = "LOAD DATA LOCAL INFILE ";
    }

    @Override
    public void writeRow(List<String> row) {

    }
}
