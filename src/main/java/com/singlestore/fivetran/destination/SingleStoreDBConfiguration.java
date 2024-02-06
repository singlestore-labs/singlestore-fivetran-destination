package com.singlestore.fivetran.destination;

import java.util.Map;

public class SingleStoreDBConfiguration {
    private final String host;
    private final Integer port;
    private final String user;
    private final String password;
    private final String sslMode;
    private final String sslKeystore;
    private final String sslKeystorePassword;
    private final String sslTruststore;
    private final String sslTruststorePassword;
    private final String sslServerCert;
    private final String driverParameters;


    SingleStoreDBConfiguration(Map<String, String> conf) {
        this.host = conf.get("host");
        this.port = Integer.valueOf(conf.get("port"));
        this.user = conf.get("user");
        this.password = conf.get("password");
        this.sslMode = conf.get("ssl.mode");
        this.sslKeystore = conf.get("ssl.keystore");
        this.sslKeystorePassword = conf.get("ssl.keystore.password");
        this.sslTruststore = conf.get("ssl.truststore");
        this.sslTruststorePassword = conf.get("ssl.truststore.password");
        this.sslServerCert = conf.get("ssl.server.cert");
        this.driverParameters = conf.get("driverParameters");
    }

    public String host() {
         return host;
    }

    public Integer port() {
        return port;
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

    public String sslKeystore() {
        return sslKeystore;
    }    

    public String sslKeystorePassword() {
        return sslKeystorePassword;
    }    

    public String sslTruststore() {
        return sslTruststore;
    }

    public String sslTruststorePassword() {
        return sslTruststorePassword;
    }

    public String sslServerCert() {
        return sslServerCert;
    }

    public String driverParameters() {
        return driverParameters;
    }
}
