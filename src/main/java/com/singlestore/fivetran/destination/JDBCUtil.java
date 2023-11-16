package com.singlestore.fivetran.destination;

import fivetran_sdk.Column;
import fivetran_sdk.CreateTableRequest;
import fivetran_sdk.DataType;
import jdk.internal.joptsimple.internal.Strings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public class JDBCUtil {
    static Connection createConnection(SingleStoreDBConfiguration conf) throws java.sql.SQLException{
        Properties connectionProps = new Properties();
        connectionProps.put("user", conf.user());
        connectionProps.put("password", conf.password());

        String url = String.format("jdbc:singlestore://%s:%d", conf.host(), conf.port());
        return DriverManager.getConnection(url, connectionProps);
    }

    static DataType mapDataTypes(Integer dataType, String typeName) {
        // TODO: map data types
        return DataType.INT;
    }

    static String generateCreateTableQuery(CreateTableRequest request) {
        String database = request.getSchemaName();
        String table = request.getTable().getName();
        String columnDefinitions = getColumnDefinitions(request.getTable().getColumnsList());

        return String.format("CREATE TABLE %s.%s (%s)",
                escapeIdentifier(database),
                escapeIdentifier(table),
                columnDefinitions
                );
    }

    static String getColumnDefinitions(List<Column> columns) {
        List<String> columnsDefinitions = columns.stream().map(
                JDBCUtil::getColumnDefinition
        ).collect(Collectors.toList());


        List<String> primaryKeyColumns = columns.stream().map(
                column -> escapeIdentifier(column.getName())
        ).collect(Collectors.toList());

        Optional<String> primaryKeyDefinition;
        if (primaryKeyColumns.isEmpty()) {
            primaryKeyDefinition = Optional.empty();
        } else {
            primaryKeyDefinition = Optional.of(
                    
            )
        }
        return Strings.join(columnsDefinitions, ",\n");
    }
    static String getColumnDefinition(Column col) {
        // TODO: handle decimal
        return String.format("%s %s", col.getName(), mapDataTypes(col.getType()));
    }

    static String mapDataTypes(DataType type) {
        // TODO: map data types
        return "INT";
    }

    static String escapeIdentifier(String ident) {
        return String.format("`%s`", ident.replace("`", "``"));
    }
}
