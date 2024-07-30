package com.singlestore.fivetran.destination.writers;

import com.github.luben.zstd.ZstdInputStream;
import com.google.protobuf.ByteString;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import fivetran_sdk.Column;
import fivetran_sdk.Compression;
import fivetran_sdk.CsvFileParams;
import fivetran_sdk.Encryption;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

abstract public class Writer {

    Connection conn;
    String database;
    String table;
    List<Column> columns;
    CsvFileParams params;
    Map<String, ByteString> secretKeys;

    public Writer(Connection conn, String database, String table, List<Column> columns,
            CsvFileParams params, Map<String, ByteString> secretKeys) {
        this.conn = conn;
        this.database = database;
        this.columns = columns;
        this.table = table;
        this.params = params;
        this.secretKeys = secretKeys;
    }

    abstract public void setHeader(List<String> header) throws SQLException;

    abstract public void writeRow(List<String> row) throws Exception;

    private IvParameterSpec readIV(FileInputStream is, String file) throws Exception {
        byte[] b = new byte[16];
        int bytesRead = 0;
        while (bytesRead != b.length) {
            int curBytesRead = is.read(b, bytesRead, b.length - bytesRead);
            if (curBytesRead == -1) {
                throw new Exception(String.format(
                        "Failed to read initialization vector. File '%s' has only %d bytes", file,
                        bytesRead));
            }
            bytesRead += curBytesRead;
        }

        return new IvParameterSpec(b);
    }

    private InputStream decodeAES(FileInputStream is, byte[] secretKeyBytes, String file)
            throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

        IvParameterSpec iv = readIV(is, file);
        SecretKey secretKey = new SecretKeySpec(secretKeyBytes, "AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, iv);

        return new CipherInputStream(is, cipher);
    }

    public void write(String file) throws Exception {
        try (FileInputStream is = new FileInputStream(file)) {
            InputStream decoded = is;
            if (params.getEncryption() == Encryption.AES) {
                decoded = decodeAES(is, secretKeys.get(file).toByteArray(), file);
            }

            InputStream uncompressed = decoded;
            if (params.getCompression() == Compression.ZSTD) {
                uncompressed = new ZstdInputStream(decoded);
            } else if (params.getCompression() == Compression.GZIP) {
                uncompressed = new GZIPInputStream(decoded);
            }

            try (CSVReader csvReader =
                    new CSVReaderBuilder(new BufferedReader(new InputStreamReader(uncompressed)))
                            .withCSVParser(new CSVParserBuilder().withEscapeChar('\0').build())
                            .build()) {
                String[] headerString = csvReader.readNext();
                if (headerString == null) {
                    // finish if file is empty
                    return;
                }

                List<String> header = new ArrayList<>(Arrays.asList(headerString));
                setHeader(header);

                String[] tokens;
                while ((tokens = csvReader.readNext()) != null) {
                    List<String> row = new ArrayList<>(Arrays.asList(tokens));
                    writeRow(row);
                }
            }

            commit();
        }
    }

    abstract public void commit() throws InterruptedException, IOException, SQLException;
}
