package com.singlestore.fivetran.destination.writers;

import fivetran_sdk.Table;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

public class UpdateWriter extends Writer {
    UpdateWriter(Connection conn, String database, Table table) {
        super(conn, database ,table);
    }

    @Override
    public void setHeader(List<String> header) {

    }

    @Override
    public void writeRow(List<String> row) {

    }

    @Override
    public void write(String file) {

    }

    @Override
    public void commit() {

    }
}
