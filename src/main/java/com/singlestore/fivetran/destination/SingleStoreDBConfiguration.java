package com.singlestore.fivetran.destination;

import java.util.Map;

public class SingleStoreDBConfiguration {
    private final String host;
    private final Integer port;
    private final String user;
    private final String password;

     SingleStoreDBConfiguration(Map<String, String> conf) {
        this.host = conf.get("host");
        this.port = Integer.valueOf(conf.get("port"));
        this.user = conf.get("user");
        this.password = conf.get("password");
        // TODO: PLAT-6891 add other configurations
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
}
