package com.singlestore.fivetran.destination;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import fivetran_sdk.TruncateRequest;

public class TruncateTest extends IntegrationTestBase {
    @Test
    public void truncate() throws SQLException, Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("USE %s", database));
            stmt.execute("CREATE TABLE truncate(a INT)");
            stmt.execute("INSERT INTO truncate VALUES (1)");
            stmt.execute("INSERT INTO truncate VALUES (10)");

            TruncateRequest request = TruncateRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTableName("truncate").build();

            String query = JDBCUtil.generateTruncateTableQuery(request);
            stmt.execute(query);

            checkResult("SELECT * FROM `truncate`", Arrays.asList());
        }
    }
}
