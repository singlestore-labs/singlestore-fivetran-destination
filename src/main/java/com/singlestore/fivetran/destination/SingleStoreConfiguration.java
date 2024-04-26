package com.singlestore.fivetran.destination;

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

    SingleStoreConfiguration(Map<String, String> conf) {
        this.host = conf.get("host");
        this.port = Integer.valueOf(conf.get("port"));
        this.database = conf.get("database");
        this.user = conf.get("user");
        this.password = conf.get("password");
        this.sslMode = conf.get("ssl.mode");
        this.sslServerCert = conf.get("ssl.server.cert");
        this.driverParameters = conf.get("driverParameters");
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
}
