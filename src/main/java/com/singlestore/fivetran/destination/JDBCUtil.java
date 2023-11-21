package com.singlestore.fivetran.destination;

import fivetran_sdk.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class JDBCUtil {
    static Connection createConnection(SingleStoreDBConfiguration conf) throws java.sql.SQLException{
        Properties connectionProps = new Properties();
        connectionProps.put("user", conf.user());
        connectionProps.put("password", conf.password());
        connectionProps.put("allowLocalInfile", "true");

        String url = String.format("jdbc:singlestore://%s:%d", conf.host(), conf.port());
        return DriverManager.getConnection(url, connectionProps);
    }

    static Table getTable(SingleStoreDBConfiguration conf, String database, String table) throws SQLException, TableNotExistException {
        try (Connection conn = JDBCUtil.createConnection(conf)) {
            DatabaseMetaData metadata = conn.getMetaData();

            try (ResultSet tables = metadata.getTables(database, null, table, null)) {
                if (!tables.next()) {
                    throw new TableNotExistException();
                }
                // TODO: PLAT-6892 print warning if several tables returned
            }

            Set<String> primaryKeys = new HashSet<>();
            try (ResultSet primaryKeysRS = metadata.getPrimaryKeys(database, null, table)) {
                while(primaryKeysRS.next()) {
                    primaryKeys.add(primaryKeysRS.getString("COLUMN_NAME"));
                }
            }

            List<Column> columns = new ArrayList<>();
            try (ResultSet columnsRS = metadata.getColumns(database, null, table, null)) {
                while(columnsRS.next()) {
                    columns.add(Column.newBuilder()
                            .setName(columnsRS.getString("COLUMN_NAME"))
                            .setType(JDBCUtil.mapDataTypes(columnsRS.getInt("DATA_TYPE"), columnsRS.getString("TYPE_NAME")))
                            .setPrimaryKey(primaryKeys.contains(columnsRS.getString("COLUMN_NAME")))
                            // TODO: PLAT-6894 get scale and precision
                            .build());
                }
            }

            return Table.newBuilder()
                    .setName(table)
                    .addAllColumns(columns)
                    .build();
        }
    }

    static DataType mapDataTypes(Integer dataType, String typeName) {
        switch (typeName) {
            case "TINYINT":
                return DataType.BOOLEAN;
            case "SMALLINT":
                return DataType.SHORT;
            case "MEDIUMINT":
            case "INT":
                return DataType.INT;
            case "BIGINT":
                return DataType.LONG;
            case "FLOAT":
                return DataType.FLOAT;
            case "DOUBLE":
                return DataType.DOUBLE;
            // TODO: PLAT-6894 handle precision and scale
            case "DECIMAL":
                return DataType.DECIMAL;
            case "DATE":
            case "YEAR":
                return DataType.NAIVE_DATE;
            case "DATETIME":
            case "TIME":
            case "TIMESTAMP":
                return DataType.NAIVE_DATETIME;
            case "BIT":
            case "BINARY":
            case "VARBINARY":
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "BLOB":
            case "LONGBLOB":
                return DataType.BINARY;
            case "CHAR":
            case "VARCHAR":
            case "TINYTEXT":
            case "MEDIUMTEXT":
            case "TEXT":
            case "LONGTEXT":
            case "GEOGRAPHYPOINT":
            case "GEOGRAPHY":
                return DataType.STRING;
            case "JSON":
                return DataType.JSON;
            default:
                return DataType.UNSPECIFIED;
        }
    }

    static String generateAlterTableQuery(AlterTableRequest request) throws SQLException, TableNotExistException {
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());

        String database = request.getSchemaName();
        String table = request.getTable().getName();

        Table oldTable = getTable(conf, database, table);
        Table newTable = request.getTable();

        // TODO: throw an exception if PK differs

        List<Column> oldColumns = oldTable.getColumnsList();
        List<Column> newColumns = newTable.getColumnsList();

        Set<Column> columnsToDrop = new HashSet<>(oldColumns);
        newColumns.forEach(columnsToDrop::remove);

        Set<Column> columnsToAdd = new HashSet<>(newColumns);
        oldColumns.forEach(columnsToAdd::remove);

        return generateAlterTableQuery(database, table, columnsToDrop, columnsToAdd);
    }

    static String generateTruncateTableQuery(TruncateRequest request) {
        return String.format("TRUNCATE TABLE %s",
                escapeTable(request.getSchemaName(), request.getTableName())
        );
    }

    // TODO: PLAT-6895 generate several queries for columnstore tables
    static String generateAlterTableQuery(String database, String table, Set<Column> columnsToDrop, Set<Column> columnsToAdd) {
        if (columnsToDrop.isEmpty() && columnsToAdd.isEmpty()) {
            return null;
        }

        List<String> operations = new ArrayList<>();

        columnsToDrop.forEach(column ->
                operations.add(String.format("DROP %s",
                        escapeIdentifier(column.getName()))));

        columnsToAdd.forEach(column ->
                operations.add(String.format("ADD %s",
                        getColumnDefinition(column))));


        return String.format("ALTER TABLE %s %s",
                escapeTable(database, table),
                String.join(", ", operations)
        );
    }

    static String generateCreateTableQuery(CreateTableRequest request) {
        String database = request.getSchemaName();
        String table = request.getTable().getName();
        String columnDefinitions = getColumnDefinitions(request.getTable().getColumnsList());

        return String.format("CREATE TABLE %s (%s)",
                escapeTable(database, table),
                columnDefinitions);
    }

    static String getColumnDefinitions(List<Column> columns) {
        List<String> columnsDefinitions = columns.stream().map(
                JDBCUtil::getColumnDefinition
        ).collect(Collectors.toList());


        List<String> primaryKeyColumns = columns.stream()
                .filter(Column::getPrimaryKey)
                .map(column -> escapeIdentifier(column.getName())
        ).collect(Collectors.toList());

        if (!primaryKeyColumns.isEmpty()) {
            columnsDefinitions.add(String.format("PRIMARY KEY (%s)",
                            String.join(", ", primaryKeyColumns)));
        }

        return String.join(",\n", columnsDefinitions);
    }

    static String getColumnDefinition(Column col) {
        // TODO: PLAT-6894 handle scale and precision
        return String.format("%s %s", escapeIdentifier(col.getName()), mapDataTypes(col.getType(), col.getDecimal()));
    }

    static String mapDataTypes(DataType type, DecimalParams decimal) {
        // TODO: PLAT-6894 handle scale and precision
        switch (type) {
            case BOOLEAN:
                return "BOOL";
            case SHORT:
                return "SMALLINT";
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case DECIMAL:
                return "DECIMAL";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case NAIVE_DATE:
                return "DATE";
            case NAIVE_DATETIME:
            case UTC_DATETIME:
                return "DATETIME";
            case BINARY:
                return "BLOB";
            case JSON:
                return "JSON";
            case UNSPECIFIED:
            case XML:
            case STRING:
            default:
                return "TEXT";
        }
    }

    public static void setParameter(PreparedStatement stmt, Integer id, DataType type, String value, String nullStr) throws SQLException {
        if (value.equals(nullStr)) {
            stmt.setNull(id, Types.NULL);
        } else {
            switch (type) {
                case BOOLEAN:
                    stmt.setBoolean(id, Boolean.parseBoolean(value));
                case SHORT:
                    stmt.setShort(id, Short.parseShort(value));
                case INT:
                    stmt.setInt(id, Integer.parseInt(value));
                case LONG:
                    stmt.setLong(id, Long.parseLong(value));
                case FLOAT:
                    stmt.setFloat(id, Float.parseFloat(value));
                case DOUBLE:
                    stmt.setDouble(id, Double.parseDouble(value));
                case BINARY:
                    stmt.setBytes(id, value.getBytes());

                case DECIMAL:
                case NAIVE_DATE:
                case NAIVE_DATETIME:
                case UTC_DATETIME:
                case XML:
                case STRING:
                case JSON:
                case UNSPECIFIED:
                default:
                    stmt.setString(id, value);
            }
        }
    }

    public static String escapeIdentifier(String ident) {
        return String.format("`%s`", ident.replace("`", "``"));
    }
    public static String escapeString(String ident) {
        return String.format("'%s'", ident.replace("'", "''"));
    }
    public static String escapeTable(String database, String table) {
        return escapeIdentifier(database) + "." + escapeIdentifier(table);
    }

    static class TableNotExistException extends Exception {
        TableNotExistException() {
            super("Table doesn't exist");
        }
    }
}
