package com.singlestore.fivetran.destination.writers;

import com.opencsv.CSVReader;
import fivetran_sdk.CsvFileParams;
import fivetran_sdk.Table;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

abstract public class Writer {

    Connection conn;
    String database;
    Table table;
    CsvFileParams params;

    public Writer(Connection conn, String database, Table table, CsvFileParams params) {
        this.conn = conn;
        this.database = database;
        this.table = table;
        this.params = params;
    }

    abstract public void setHeader(List<String> header) throws SQLException;
    abstract public void writeRow(List<String> row) throws Exception;

    public void write(String file) throws Exception {
        // TODO: PLAT-6896 decrypt and uncompress files
        try (
                Reader reader = Files.newBufferedReader(Paths.get(file));
                CSVReader csvReader = new CSVReader(reader)
        ) {
            List<String> header = new ArrayList<>(Arrays.asList(csvReader.readNext()));
            // delete _fivetran_synced
            header.remove(header.size() - 1);
            // delete _fivetran_deleted
            header.remove(header.size() - 1);
            setHeader(header);

            String[] tokens;
            while ((tokens = csvReader.readNext()) != null) {
                List<String> row = new ArrayList<>(Arrays.asList(tokens));
                // delete _fivetran_synced
                row.remove(row.size() - 1);
                // delete _fivetran_deleted
                row.remove(row.size() - 1);
                writeRow(row);
            }
        }
    }

    abstract public void commit() throws InterruptedException, IOException, SQLException;
}
