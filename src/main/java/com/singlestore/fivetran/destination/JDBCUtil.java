package com.singlestore.fivetran.destination;

import fivetran_sdk.*;

import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCUtil {
    private static final Logger logger = LoggerFactory.getLogger(JDBCUtil.class);

    static Connection createConnection(SingleStoreConfiguration conf) throws Exception {
        Properties connectionProps = new Properties();
        connectionProps.put("user", conf.user());
        if (conf.password() != null) {
            connectionProps.put("password", conf.password());
        }
        connectionProps.put("allowLocalInfile", "true");
        connectionProps.put("transformedBitIsBoolean", "true");
        connectionProps.put("allowMultiQueries", "true");
        connectionProps.put("connectionAttributes",
                String.format("_connector_name:%s,_connector_version:%s",
                        "SingleStore Fivetran Destination", VersionProvider.getVersion()));

        connectionProps.put("sslMode", conf.sslMode());
        if (!conf.sslMode().equals("disable")) {
            putIfNotEmpty(connectionProps, "serverSslCert", conf.sslServerCert());
        }

        String driverParameters = conf.driverParameters();
        if (driverParameters != null) {
            for (String parameter : driverParameters.split(";")) {
                String[] keyValue = parameter.split("=");
                if (keyValue.length != 2) {
                    throw new Exception("Invalid value of `driverParameters` configuration");
                }
                putIfNotEmpty(connectionProps, keyValue[0], keyValue[1]);
            }
        }

        String url = String.format("jdbc:singlestore://%s:%d", conf.host(), conf.port());

        try {
            return DriverManager.getConnection(url, connectionProps);
        } catch (SQLException e) {
            if (e.getErrorCode() == 1046 && e.getSQLState().equals("3D000")
                    && conf.database() != null) {
                url = String.format("jdbc:singlestore://%s:%d/%s", conf.host(), conf.port(),
                        conf.database());
                return DriverManager.getConnection(url, connectionProps);
            }

            throw e;
        }
    }

    private static void putIfNotEmpty(Properties props, String key, String value) {
        if (key != null && !key.trim().isEmpty() && value != null && !value.trim().isEmpty()) {
            props.put(key.trim(), value.trim());
        }
    }

    static boolean checkTableExists(Statement stmt, String database, String table) {
        try {
            stmt.executeQuery(
                    String.format("SELECT * FROM %s WHERE 1=0", escapeTable(database, table)));
        } catch (Exception ignored) {
            return false;
        }
        return true;
    }

    static boolean checkDatabaseExists(Statement stmt, String database) throws SQLException {
        try (ResultSet rs = stmt
                .executeQuery(String.format("SHOW DATABASES LIKE %s", escapeString(database)))) {
            return rs.next();
        }
    }

    static Table getTable(SingleStoreConfiguration conf, String database, String table,
            String originalTableName) throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf)) {
            DatabaseMetaData metadata = conn.getMetaData();

            try (ResultSet tables = metadata.getTables(database, null, table, null)) {
                if (!tables.next()) {
                    throw new TableNotExistException();
                }
                if (tables.next()) {
                    logger.warn(String.format("Found several tables that match %s name",
                            JDBCUtil.escapeTable(database, table)));
                }
            }

            Set<String> primaryKeys = new HashSet<>();
            try (ResultSet primaryKeysRS = metadata.getPrimaryKeys(database, null, table)) {
                while (primaryKeysRS.next()) {
                    primaryKeys.add(primaryKeysRS.getString("COLUMN_NAME"));
                }
            }

            List<Column> columns = new ArrayList<>();
            try (ResultSet columnsRS = metadata.getColumns(database, null, table, null)) {
                while (columnsRS.next()) {
                    Column.Builder c = Column.newBuilder()
                            .setName(columnsRS.getString("COLUMN_NAME"))
                            .setType(JDBCUtil.mapDataTypes(columnsRS.getInt("DATA_TYPE"),
                                    columnsRS.getString("TYPE_NAME")))
                            .setPrimaryKey(
                                    primaryKeys.contains(columnsRS.getString("COLUMN_NAME")));
                    if (c.getType() == DataType.DECIMAL) {
                        c.setDecimal(DecimalParams.newBuilder()
                                .setScale(columnsRS.getInt("DECIMAL_DIGITS"))
                                .setPrecision(columnsRS.getInt("COLUMN_SIZE")).build());
                    }
                    columns.add(c.build());
                }
            }

            return Table.newBuilder().setName(originalTableName).addAllColumns(columns).build();
        }
    }

    static DataType mapDataTypes(Integer dataType, String typeName) {
        switch (typeName) {
            case "BOOLEAN":
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

    static private Set<String> pkColumnNames(Table table) {
        return table.getColumnsList().stream().filter(column -> column.getPrimaryKey())
                .map(column -> column.getName()).collect(Collectors.toSet());
    }

    static private boolean pkEquals(Table t1, Table t2) {
        return pkColumnNames(t1).equals(pkColumnNames(t2));
    }

    static String generateAlterTableQuery(AlterTableRequest request) throws Exception {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(request.getConfigurationMap());

        String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table =
                JDBCUtil.getTableName(conf, request.getSchemaName(), request.getTable().getName());

        Table oldTable = getTable(conf, database, table, request.getTable().getName());
        Table newTable = request.getTable();
        boolean pkChanged = false;

        if (!pkEquals(oldTable, newTable)) {
            pkChanged = true;
        }

        Map<String, Column> oldColumns = oldTable.getColumnsList().stream()
                .collect(Collectors.toMap(Column::getName, Function.identity()));

        List<Column> columnsToAdd = new ArrayList<>();
        List<Column> columnsToChange = new ArrayList<>();
        List<Column> commonColumns = new ArrayList<>();

        for (Column column : newTable.getColumnsList()) {
            Column oldColumn = oldColumns.get(column.getName());
            if (oldColumn == null) {
                columnsToAdd.add(column);
            } else {
                commonColumns.add(column);
                String oldType = mapDataTypes(oldColumn.getType(), oldColumn.getDecimal());
                String newType = mapDataTypes(column.getType(), column.getDecimal());
                if (!oldType.equals(newType)) {
                    if (oldColumn.getPrimaryKey()) {
                        pkChanged = true;
                        continue;
                    }
                    columnsToChange.add(column);
                }
            }
        }

        if (pkChanged) {
            logger.warn(
                    "Alter table changes the key of the table. This operation is not supported by SingleStore. The table will be recreated from scratch.");

            return generateRecreateTableQuery(database, table, newTable, commonColumns);
        } else {
            return generateAlterTableQuery(database, table, columnsToAdd, columnsToChange);
        }
    }

    static String generateRecreateTableQuery(String database, String tableName, Table table,
            List<Column> commonColumns) {
        String tmpTableName = tableName + "_alter_tmp";
        String columns = commonColumns.stream().map(column -> escapeIdentifier(column.getName()))
                .collect(Collectors.joining(", "));

        String createTable = generateCreateTableQuery(database, tmpTableName, table);
        String insertData = String.format("INSERT INTO %s (%s) SELECT %s FROM %s",
                escapeTable(database, tmpTableName), columns, columns,
                escapeTable(database, tableName));
        String dropTable = String.format("DROP TABLE %s", escapeTable(database, tableName));
        String renameTable = String.format("ALTER TABLE %s RENAME AS %s",
                escapeTable(database, tmpTableName), escapeTable(database, tableName));

        return String.join("; ", createTable, insertData, dropTable, renameTable);
    }

    static String generateTruncateTableQuery(SingleStoreConfiguration conf,
            TruncateRequest request) {
        String query;
        String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = JDBCUtil.getTableName(conf, request.getSchemaName(), request.getTableName());

        if (request.hasSoft()) {
            query = String.format("UPDATE %s SET %s = 1 ", escapeTable(database, table),
                    escapeIdentifier(request.getSoft().getDeletedColumn()));
        } else {
            query = String.format("DELETE FROM %s ", escapeTable(database, table));
        }

        query += String.format("WHERE %s < FROM_UNIXTIME(%d.%09d)",
                escapeIdentifier(request.getSyncedColumn()),
                request.getUtcDeleteBefore().getSeconds(), request.getUtcDeleteBefore().getNanos());

        return query;
    }

    static String generateAlterTableQuery(String database, String table, List<Column> columnsToAdd,
            List<Column> columnsToChange) {
        if (columnsToAdd.isEmpty() && columnsToChange.isEmpty()) {
            return null;
        }

        StringBuilder query = new StringBuilder();

        for (Column column : columnsToChange) {
            String tmpColName = column.getName() + "_alter_tmp";
            query.append(String.format("ALTER TABLE %s ADD COLUMN %s %s; ",
                    escapeTable(database, table), escapeIdentifier(tmpColName),
                    mapDataTypes(column.getType(), column.getDecimal())));
            query.append(
                    String.format("UPDATE %s SET %s = %s :> %s; ", escapeTable(database, table),
                            escapeIdentifier(tmpColName), escapeIdentifier(column.getName()),
                            mapDataTypes(column.getType(), column.getDecimal())));
            query.append(String.format("ALTER TABLE %s DROP %s; ", escapeTable(database, table),
                    escapeIdentifier(column.getName())));
            query.append(String.format("ALTER TABLE %s CHANGE %s %s; ",
                    escapeTable(database, table), tmpColName, escapeIdentifier(column.getName())));
        }

        if (!columnsToAdd.isEmpty()) {
            List<String> addOperations = new ArrayList<>();

            columnsToAdd.forEach(column -> addOperations
                    .add(String.format("ADD %s", getColumnDefinition(column))));

            query.append(String.format("ALTER TABLE %s %s; ", escapeTable(database, table),
                    String.join(", ", addOperations)));
        }

        return query.toString();
    }

    static String generateCreateTableQuery(String database, String tableName, Table table) {
        String columnDefinitions = getColumnDefinitions(table.getColumnsList());
        return String.format("CREATE TABLE %s (%s)", escapeTable(database, tableName),
                columnDefinitions);
    }

    static String generateCreateTableQuery(SingleStoreConfiguration conf, Statement stmt,
            CreateTableRequest request) throws SQLException {
        String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table =
                JDBCUtil.getTableName(conf, request.getSchemaName(), request.getTable().getName());
        String createTableQuery = generateCreateTableQuery(database, table, request.getTable());

        if (!checkDatabaseExists(stmt, database)) {
            return String.format("CREATE DATABASE IF NOT EXISTS %s; %s", escapeIdentifier(database),
                    createTableQuery);
        } else {
            return createTableQuery;
        }
    }

    static String getColumnDefinitions(List<Column> columns) {
        List<String> columnsDefinitions =
                columns.stream().map(JDBCUtil::getColumnDefinition).collect(Collectors.toList());

        List<String> primaryKeyColumns = columns.stream().filter(Column::getPrimaryKey)
                .map(column -> escapeIdentifier(column.getName())).collect(Collectors.toList());

        if (!primaryKeyColumns.isEmpty()) {
            columnsDefinitions
                    .add(String.format("PRIMARY KEY (%s)", String.join(", ", primaryKeyColumns)));
        }

        return String.join(",\n", columnsDefinitions);
    }

    static String getColumnDefinition(Column col) {
        return String.format("%s %s", escapeIdentifier(col.getName()),
                mapDataTypes(col.getType(), col.getDecimal()));
    }

    static String mapDataTypes(DataType type, DecimalParams decimal) {
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
                if (decimal != null) {
                    return String.format("DECIMAL (%d, %d)", decimal.getPrecision(),
                            Math.min(30, decimal.getScale()));
                }
                return "DECIMAL";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case NAIVE_DATE:
                return "DATE";
            case NAIVE_DATETIME:
            case UTC_DATETIME:
                return "DATETIME(6)";
            case BINARY:
                return "BLOB";
            case JSON:
                return "JSON";
            case UNSPECIFIED:
            case XML:
            case STRING:
            default:
                return "TEXT CHARACTER SET utf8mb4";
        }
    }

    public static String formatISODateTime(String dateTime) {
        dateTime = dateTime.replace("T", " ").replace("Z", "");
        // SingleStore doesn't support more than 6 digits after a period
        int dotPos = dateTime.indexOf(".", 0);
        if (dotPos != -1 && dotPos + 6 < dateTime.length()) {
            return dateTime.substring(0, dotPos + 6 + 1);
        }
        return dateTime;
    }

    public static void setParameter(PreparedStatement stmt, Integer id, DataType type, String value,
            String nullStr) throws SQLException {
        if (value.equals(nullStr)) {
            stmt.setNull(id, Types.NULL);
        } else {
            switch (type) {
                case BOOLEAN:
                    if (value.equalsIgnoreCase("true")) {
                        stmt.setBoolean(id, true);
                    } else if (value.equalsIgnoreCase("false")) {
                        stmt.setBoolean(id, false);
                    } else {
                        stmt.setShort(id, Short.parseShort(value));
                    }
                    break;
                case SHORT:
                    stmt.setShort(id, Short.parseShort(value));
                    break;
                case INT:
                    stmt.setInt(id, Integer.parseInt(value));
                    break;
                case LONG:
                    stmt.setLong(id, Long.parseLong(value));
                    break;
                case FLOAT:
                    stmt.setFloat(id, Float.parseFloat(value));
                    break;
                case DOUBLE:
                    stmt.setDouble(id, Double.parseDouble(value));
                    break;
                case BINARY:
                    stmt.setBytes(id, Base64.getDecoder().decode(value));
                    break;

                case NAIVE_DATETIME:
                case UTC_DATETIME:
                    stmt.setString(id, formatISODateTime(value));
                    break;

                case DECIMAL:
                case NAIVE_DATE:
                case XML:
                case STRING:
                case JSON:
                case UNSPECIFIED:
                default:
                    stmt.setString(id, value);
                    break;
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

    public static String getDatabaseName(SingleStoreConfiguration conf, String schema) {
        if (conf.database() != null) {
            return conf.database();
        } else {
            return schema;
        }
    }

    public static String getTableName(SingleStoreConfiguration conf, String schema, String table) {
        if (conf.database() != null) {
            return String.format("%s__%s", schema, table);
        } else {
            return table;
        }
    }
}
