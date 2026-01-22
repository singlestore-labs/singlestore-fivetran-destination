package com.singlestore.fivetran.destination.connector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SingleStoreConfiguration {
    private final String host;
    private final Integer port;
    private final String database;
    private final String user;
    private final String password;
    private final String sslMode;
    private final String sslServerCert;
    private final String driverParameters;
    private final Integer batchSize;
    private final Map<String, String> fivetranSchemaToSingleStoreDatabase = new HashMap<>();

    SingleStoreConfiguration(Map<String, String> conf) {
        this.host = conf.get("host");
        this.port = Integer.valueOf(conf.get("port"));
        this.database = withDefaultNull(conf.get("database"));
        this.user = conf.get("user");
        this.password = withDefaultNull(conf.get("password"));
        this.sslMode = withDefault(conf.get("ssl.mode"), "disable");
        this.sslServerCert = formatServerCert(withDefaultNull(conf.get("ssl.server.cert")));
        this.driverParameters = withDefaultNull(conf.get("driver.parameters"));
        this.batchSize = Integer.valueOf(withDefault(conf.get("batch.size"), "10000"));
        String databaseNameMapping = withDefault(conf.get("database.name.mapping"), "");
        Arrays.stream(databaseNameMapping.split(";")).forEach(mapping -> {
            if (mapping.isEmpty()) {
                return;
            }

            String[] parts = mapping.split("=");
            if (parts.length == 2) {
                String fivetranSchema = parts[0].trim();
                String singleStoreDatabase = parts[1].trim();
                fivetranSchemaToSingleStoreDatabase.put(fivetranSchema, singleStoreDatabase);
            } else {
                throw new IllegalArgumentException(String.format("Invalid database name mapping: %s", mapping));
            }
        });
    }

    private String formatServerCert(String cert) {
        if (cert == null) {
            return cert;
        }

        return cert.replace("-----BEGIN CERTIFICATE-----", "-----BEGIN-CERTIFICATE-----")
                .replace("-----END CERTIFICATE-----", "-----END-CERTIFICATE-----")
                .replace(" ", "\n")
                .replace("-----BEGIN-CERTIFICATE-----", "-----BEGIN CERTIFICATE-----")
                .replace("-----END-CERTIFICATE-----", "-----END CERTIFICATE-----");
    }

    private String withDefault(String s, String def) {
        if (s == null || s.isEmpty()) {
            return def;
        }

        return s;
    }

    private String withDefaultNull(String s) {
        return withDefault(s, null);
    }

    public String host() {
        return host;
    }

    public Integer port() {
        return port;
    }

    public String database() {
        return database;
    }

    public String user() {
        return user;
    }

    public String password() {
        return password;
    }

    public String sslMode() {
        return sslMode;
    }

    public String sslServerCert() {
        return sslServerCert;
    }

    public String driverParameters() {
        return driverParameters;
    }

    public Integer batchSize() {
        return batchSize;
    }

    public String getSingleStoreDatabase(String fivetranSchema) {
        return fivetranSchemaToSingleStoreDatabase.getOrDefault(fivetranSchema, fivetranSchema);
    }
}
