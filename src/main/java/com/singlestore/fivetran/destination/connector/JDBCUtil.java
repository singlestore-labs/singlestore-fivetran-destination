package com.singlestore.fivetran.destination.connector;

import com.singlestore.fivetran.destination.connector.warning_util.WarningHandler;
import fivetran_sdk.v2.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
                        "SingleStore Fivetran Destination Connector", VersionProvider.getVersion()));

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
            try {
                return DriverManager.getConnection(url, connectionProps);
            } catch (SQLException e) {
                // Error 1046 (SQLSTATE 3D000) = "No database selected".
                // This typically means the user account does not have permission
                // to connect without specifying a default database.
                // If a database is provided in the configuration, retry the connection
                // by explicitly including it in the JDBC URL.
                if (e.getErrorCode() == 1046 && e.getSQLState().equals("3D000")
                        && conf.database() != null) {
                    url = String.format("jdbc:singlestore://%s:%d/%s", conf.host(), conf.port(),
                            conf.database());
                    return DriverManager.getConnection(url, connectionProps);
                }

                throw e;
            }
        } catch (SQLNonTransientConnectionException e) {
            // Catch block to provide a clearer error message.
            // A "Socket fail to connect" error indicates that the connector could not
            // establish a connection to the database.
            // Common causes include invalid credentials or firewall/network restrictions.
            String message = e.getMessage();
            if (message != null && message.contains("Socket fail to connect")) {
                String host = conf.host();
                Integer port = conf.port();

                String friendly = String.format(
                        "Failed to connect to SingleStore at %s:%d.\n\n" +
                                "Try these steps to resolve it:\n" +
                                "  1) Verify your SingleStore cluster is running and listening on port %d.\n" +
                                "  2) Confirm Fivetran’s IPs for your region are allowlisted in your firewall: https://fivetran.com/docs/using-fivetran/ips\n" +
                                "  3) Ensure the hostname and port in the destination configuration are correct (host=%s, port=%d).\n\n" +
                                "Original error: %s",
                        host, port,
                        port,
                        host, port,
                        e.getMessage()
                );

                // Preserve SQLState & vendor error code, and keep the original cause/stack
                throw new SQLNonTransientConnectionException(
                        friendly,
                        e.getSQLState(),
                        e.getErrorCode(),
                        e
                );
            } else {
                throw e;
            }
        }
    }

    public static class QueryWithCleanup {
        private final String query;
        private final String cleanupQuery; // nullable
        private final String warningMessage;
        private final List<String> parameterValues = new ArrayList<>();
        private final List<DataType> parameterTypes = new ArrayList<>();


        public QueryWithCleanup(String query, String cleanupQuery, String warningMessage) {
            this.query = query;
            this.cleanupQuery = cleanupQuery;
            this.warningMessage = warningMessage;
        }

        public QueryWithCleanup addParameter(String value, DataType type) {
            parameterValues.add(value.toString());
            parameterTypes.add(type);
            return this;
        }

        public String getQuery() {
            return query;
        }

        public String getCleanupQuery() {
            return cleanupQuery;
        }

        public String getWarningMessage() {
            return warningMessage;
        }

        public void execute(Connection conn) throws SQLException {
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                for (int i = 0; i < parameterTypes.size(); i++) {
                    String value = parameterValues.get(i);
                    DataType type = parameterTypes.get(i);
                    JDBCUtil.setParameter(stmt, i + 1, type, value, "NULL");
                }
                stmt.execute();
            }
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


    static <T> Table getTable(SingleStoreConfiguration conf, String database, String table,
                              String originalTableName, WarningHandler warningHandler) throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf)) {
            DatabaseMetaData metadata = conn.getMetaData();

            try (ResultSet tables = metadata.getTables(database, null, table, null)) {
                if (!tables.next()) {
                    throw new TableNotExistException();
                }
                if (tables.next()) {
                    warningHandler.handle(String.format("Found several tables that match %s name",
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
                        c.setParams(DataTypeParams.newBuilder()
                                .setDecimal(DecimalParams.newBuilder()
                                        .setScale(columnsRS.getInt("DECIMAL_DIGITS"))
                                        .setPrecision(columnsRS.getInt("COLUMN_SIZE")).build())
                                .build());
                    }
                    if (c.getType() == DataType.STRING) {
                        String typeName = columnsRS.getString("TYPE_NAME");
                        if (typeName.equals("GEOGRAPHYPOINT") || typeName.equals("GEOGRAPHY")) {
                            c.setParams(DataTypeParams.newBuilder()
                                    .setStringByteLength(Integer.MAX_VALUE)
                                    .build());
                        } else {
                            c.setParams(DataTypeParams.newBuilder()
                                    .setStringByteLength(columnsRS.getInt("CHAR_OCTET_LENGTH"))
                                    .build());
                        }
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

    static <T> List<QueryWithCleanup> generateAlterTableQuery(AlterTableRequest request, WarningHandler warningHandler) throws Exception {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(request.getConfigurationMap());

        String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table =
                JDBCUtil.getTableName(conf, request.getSchemaName(), request.getTable().getName());

        Table oldTable = getTable(conf, database, table, request.getTable().getName(), warningHandler);
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
                String oldType = mapDataTypes(oldColumn.getType(), oldColumn.getParams());
                String newType = mapDataTypes(column.getType(), column.getParams());
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
            warningHandler.handle("Alter table changes the key of the table. This operation is not supported by SingleStore. The table will be recreated from scratch.");

            return generateRecreateTableQuery(database, table, newTable, commonColumns);
        } else {
            return generateAlterTableQuery(database, table, columnsToAdd, columnsToChange);
        }
    }

    static List<QueryWithCleanup> generateRecreateTableQuery(String database, String tableName, Table table,
                                                             List<Column> commonColumns) {
        String tmpTableName = getTempName(tableName);
        String columns = commonColumns.stream().map(column -> escapeIdentifier(column.getName()))
                .collect(Collectors.joining(", "));

        String createTable = generateCreateTableQuery(database, tmpTableName, table);
        String cleanupTmpTable = String.format("DROP TABLE IF EXISTS %s",
                escapeTable(database, tmpTableName));
        String insertData = String.format("INSERT INTO %s (%s) SELECT %s FROM %s",
                escapeTable(database, tmpTableName), columns, columns,
                escapeTable(database, tableName));
        String dropTable = String.format("DROP TABLE %s", escapeTable(database, tableName));
        String renameTable = String.format("ALTER TABLE %s RENAME AS %s",
                escapeTable(database, tmpTableName), escapeTable(database, tableName));

        return Arrays.asList(
                new QueryWithCleanup(createTable, null, null),
                new QueryWithCleanup(insertData, cleanupTmpTable, null),
                new QueryWithCleanup(dropTable, cleanupTmpTable, null),
                // The original table has been dropped; all data now resides in the temporary table.
                new QueryWithCleanup(renameTable, null,
                        String.format("Failed to recreate table %s with the new schema. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, tableName),
                                escapeTable(database, tmpTableName),
                                escapeTable(database, tmpTableName),
                                escapeTable(database, tableName)))
        );
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

    static List<QueryWithCleanup> generateAlterTableQuery(String database, String table, List<Column> columnsToAdd,
                                                          List<Column> columnsToChange) {
        if (columnsToAdd.isEmpty() && columnsToChange.isEmpty()) {
            return null;
        }

        List<QueryWithCleanup> queries = new ArrayList<>();

        for (Column column : columnsToChange) {
            String tmpColName = getTempName(column.getName());

            String addColumnQuery = String.format("ALTER TABLE %s ADD COLUMN %s %s",
                    escapeTable(database, table), escapeIdentifier(tmpColName),
                    mapDataTypes(column.getType(), column.getParams()));
            String cleanupTmpColumnQuery = String.format("ALTER TABLE %s DROP %s",
                    escapeTable(database, table), escapeIdentifier(tmpColName));
            String copyDataQuery = String.format("UPDATE %s SET %s = %s :> %s",
                    escapeTable(database, table), escapeIdentifier(tmpColName),
                    escapeIdentifier(column.getName()), mapDataTypes(column.getType(), column.getParams()));
            String dropColumnQuery = String.format("ALTER TABLE %s DROP %s",
                    escapeTable(database, table), escapeIdentifier(column.getName()));
            String renameColumnQuery = String.format("ALTER TABLE %s CHANGE %s %s; ",
                    escapeTable(database, table), tmpColName, escapeIdentifier(column.getName()));

            queries.add(new QueryWithCleanup(addColumnQuery, null, null));
            queries.add(new QueryWithCleanup(copyDataQuery, cleanupTmpColumnQuery, null));
            queries.add(new QueryWithCleanup(dropColumnQuery, cleanupTmpColumnQuery, null));
            queries.add(new QueryWithCleanup(renameColumnQuery, null, null));
        }

        if (!columnsToAdd.isEmpty()) {
            List<String> addOperations = new ArrayList<>();

            columnsToAdd.forEach(column -> addOperations
                    .add(String.format("ADD %s", getColumnDefinition(column))));

            String addColumnsQuery = String.format("ALTER TABLE %s %s; ",
                    escapeTable(database, table), String.join(", ", addOperations));
            queries.add(new QueryWithCleanup(addColumnsQuery, null, null));
        }

        return queries;
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
                mapDataTypes(col.getType(), col.getParams()));
    }

    static String mapDataTypes(DataType type, DataTypeParams params) {
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
                if (params != null && params.getDecimal() != null) {
                    DecimalParams decimalParams = params.getDecimal();
                    return String.format("DECIMAL (%d, %d)", decimalParams.getPrecision(),
                            Math.min(30, decimalParams.getScale()));
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
                if (params != null && params.getStringByteLength() != 0) {
                    int stringByteLength = params.getStringByteLength();
                    if (stringByteLength <= 255) {
                        return "TINYTEXT CHARACTER SET utf8mb4";
                    } else if (stringByteLength <= 65535) {
                        return "TEXT CHARACTER SET utf8mb4";
                    } else if (stringByteLength <= 16777215) {
                        return "MEDIUMTEXT CHARACTER SET utf8mb4";
                    } else {
                        return "LONGTEXT CHARACTER SET utf8mb4";
                    }
                }

                return "LONGTEXT CHARACTER SET utf8mb4";
        }
    }

    private static Timestamp toTimestamp(String dateTime) {
        // Define the formatter for microsecond precision
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

        // Convert to LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, formatter);

        // Convert LocalDateTime to Timestamp
        return Timestamp.valueOf(localDateTime);
    }

    public static String formatISODateTime(String dateTime) {
        dateTime = dateTime.replace("T", " ").replace("Z", "");

        // We want all dates to have exactly 6 digits after the dot
        int dotPos = dateTime.indexOf('.');
        if (dotPos == -1) {
            return dateTime + ".000000";
        }

        int digitsAfterDot = dateTime.length() - dotPos - 1;
        if (digitsAfterDot >= 6) {
            // Trim if more than 6 digits
            return dateTime.substring(0, dotPos + 7);
        }

        // Append missing zeros to make it exactly 6 digits
        return dateTime + "000000".substring(digitsAfterDot);
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

    private static String getTempName(String originalName) {
        return originalName + "_tmp_" + Integer.toHexString(new Random().nextInt(0x1000000));
    }

    private static boolean checkTableNonEmpty(SingleStoreConfiguration conf, String database, String table) throws Exception {
        try (Connection conn = JDBCUtil.createConnection(conf);
            Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("SELECT 1 FROM %s LIMIT 1", escapeTable(database, table)))) {
            return rs.next();
        }
    }

    private static boolean checkMaxStartTime(SingleStoreConfiguration conf, String database, String table, String maxTime) throws Exception {
        try (
            Connection conn = JDBCUtil.createConnection(conf);
            PreparedStatement stmt = conn.prepareStatement(
                String.format("SELECT MAX(_fivetran_start) < ? FROM %s", escapeTable(database, table)));
        ) {
            setParameter(stmt, 1, DataType.NAIVE_DATETIME, maxTime, "NULL");
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    static List<QueryWithCleanup> generateMigrateQueries(MigrateRequest request, WarningHandler warningHandler) throws Exception {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(request.getConfigurationMap());

        MigrationDetails details = request.getDetails();
        String database = JDBCUtil.getDatabaseName(conf, details.getSchema());
        String table =
                JDBCUtil.getTableName(conf, details.getSchema(), details.getTable());

        Table t;
        switch (details.getOperationCase()) {
            case DROP:
                DropOperation drop = details.getDrop();
                switch (drop.getEntityCase()) {
                    case DROP_TABLE:
                        return generateMigrateDropQueries(table, database);
                    case DROP_COLUMN_IN_HISTORY_MODE:
                        DropColumnInHistoryMode dropColumnInHistoryMode = drop.getDropColumnInHistoryMode();

                        if (!checkTableNonEmpty(conf, database, table)) {
                            return new ArrayList<>();
                        }
                        if (!checkMaxStartTime(conf, database, table, dropColumnInHistoryMode.getOperationTimestamp())) {
                            throw new IllegalArgumentException("Cannot drop column in history mode because maximum _fivetran_start is greater than the operation timestamp");
                        }

                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateDropColumnInHistoryMode(drop.getDropColumnInHistoryMode(), t, database, table);
                    default:
                        throw new IllegalArgumentException("Unsupported drop operation");
                }
            case COPY:
                CopyOperation copy = details.getCopy();
                switch (copy.getEntityCase()) {
                    case COPY_TABLE:
                        CopyTable renameTableMigration = copy.getCopyTable();
                        String tableFrom =
                                JDBCUtil.getTableName(conf, details.getSchema(), renameTableMigration.getFromTable());
                        String tableTo =
                                JDBCUtil.getTableName(conf, details.getSchema(), renameTableMigration.getToTable());

                        return generateMigrateCopyTable(tableFrom, tableTo, database);
                    case COPY_COLUMN:
                        CopyColumn migration = copy.getCopyColumn();
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        Column c = t.getColumnsList().stream()
                                .filter(column -> column.getName().equals(migration.getFromColumn()))
                                .findFirst()
                                .orElseThrow(() -> new IllegalArgumentException("Source column doesn't exist"));

                        return generateMigrateCopyColumn(copy.getCopyColumn(), database, table, c);
                    case COPY_TABLE_TO_HISTORY_MODE:
                        CopyTableToHistoryMode copyTableToHistoryModeMigration = copy.getCopyTableToHistoryMode();
                        String tableFromHM =
                                JDBCUtil.getTableName(conf, details.getSchema(), copyTableToHistoryModeMigration.getFromTable());
                        String tableToHM =
                                JDBCUtil.getTableName(conf, details.getSchema(), copyTableToHistoryModeMigration.getToTable());

                        t = getTable(conf, database, tableFromHM, copyTableToHistoryModeMigration.getFromTable(), warningHandler);

                        return generateMigrateCopyTableToHistoryMode(t,
                                database, tableFromHM, tableToHM, copyTableToHistoryModeMigration.getSoftDeletedColumn());
                    default:
                        throw new IllegalArgumentException("Unsupported copy operation");
                }
            case RENAME:
                RenameOperation rename = details.getRename();
                switch (rename.getEntityCase()) {
                    case RENAME_TABLE:
                        RenameTable renameTableMigration = rename.getRenameTable();
                        String tableFrom =
                                JDBCUtil.getTableName(conf, details.getSchema(), renameTableMigration.getFromTable());
                        String tableTo =
                                JDBCUtil.getTableName(conf, details.getSchema(), renameTableMigration.getToTable());

                        return generateMigrateRenameTable(tableFrom, tableTo, database);
                    case RENAME_COLUMN:
                        return generateMigrateRenameColumn(rename.getRenameColumn(), database, table);
                    default:
                        throw new IllegalArgumentException("Unsupported rename operation");
                }
            case ADD:
                AddOperation add = details.getAdd();
                switch (add.getEntityCase()) {
                    case ADD_COLUMN_IN_HISTORY_MODE:
                        AddColumnInHistoryMode addColumnInHistoryMode = add.getAddColumnInHistoryMode();

                        boolean isEmpty = !checkTableNonEmpty(conf, database, table);
                        if (!isEmpty && !checkMaxStartTime(conf, database, table, addColumnInHistoryMode.getOperationTimestamp())) {
                            throw new IllegalArgumentException("Cannot add column in history mode because maximum _fivetran_start is greater than the operation timestamp");
                        }

                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateAddColumnInHistoryMode(addColumnInHistoryMode, t, database, table, isEmpty);
                    case ADD_COLUMN_WITH_DEFAULT_VALUE:
                        return generateMigrateAddColumnWithDefaultValue(add.getAddColumnWithDefaultValue(), table, database);
                    default:
                        throw new IllegalArgumentException("Unsupported add operation");
                }
            case UPDATE_COLUMN_VALUE:
                UpdateColumnValueOperation updateColumnValue = details.getUpdateColumnValue();
                t = getTable(conf, database, table, details.getTable(), warningHandler);
                Column c = t.getColumnsList().stream()
                        .filter(column -> column.getName().equals(updateColumnValue.getColumn()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Source column doesn't exist"));

                return generateMigrateUpdateColumnValueOperation(updateColumnValue, database, table, c.getType());
            case TABLE_SYNC_MODE_MIGRATION:
                TableSyncModeMigrationOperation tableSyncModeMigration = details.getTableSyncModeMigration();
                TableSyncModeMigrationType type = tableSyncModeMigration.getType();
                String softDeleteColumn = tableSyncModeMigration.getSoftDeletedColumn();
                Boolean keepDeletedRows = tableSyncModeMigration.getKeepDeletedRows();
                switch (type) {
                    case SOFT_DELETE_TO_LIVE:
                        return generateMigrateSoftDeleteToLive(database, table, softDeleteColumn);
                    case SOFT_DELETE_TO_HISTORY:
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateMigrateSoftDeleteToHistory(t, database, table, softDeleteColumn);
                    case HISTORY_TO_SOFT_DELETE:
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateMigrateHistoryToSoftDelete(t, database, table, softDeleteColumn);
                    case HISTORY_TO_LIVE:
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateMigrateHistoryToLive(t, database, table, keepDeletedRows);
                    case LIVE_TO_HISTORY:
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateMigrateLiveToHistory(t, database, table);
                    case LIVE_TO_SOFT_DELETE:
                        return generateMigrateLiveToSoftDelete(database, table, softDeleteColumn);
                    default:
                        throw new IllegalArgumentException("Unsupported table sync mode migration operation");
                }
            default:
                throw new IllegalArgumentException("Unsupported migration operation");
        }
    }

    static List<QueryWithCleanup> generateMigrateDropQueries(String table, String database) {
        String query = String.format("DROP TABLE IF EXISTS %s", escapeTable(database, table));
        return Collections.singletonList(new QueryWithCleanup(query, null, null));
    }

    static List<QueryWithCleanup> generateMigrateAddColumnWithDefaultValue(AddColumnWithDefaultValue migration, String table, String database) {
        String column = migration.getColumn();
        DataType type = migration.getColumnType();
        String defaultValue = migration.getDefaultValue();
        String query = String.format("ALTER TABLE %s ADD COLUMN %s %s DEFAULT ?",
                escapeTable(database, table), escapeIdentifier(column),
                mapDataTypes(type, null));

        QueryWithCleanup alterQuery = new QueryWithCleanup(query, null, null);
        alterQuery.addParameter(defaultValue, type);
        return Collections.singletonList(alterQuery);
    }

    static List<QueryWithCleanup> generateMigrateRenameTable(String tableFrom, String tableTo, String database) {
        String query = String.format("ALTER TABLE %s RENAME %s", escapeTable(database, tableFrom), escapeIdentifier(tableTo));
        return Collections.singletonList(new QueryWithCleanup(query, null, null));
    }

    static List<QueryWithCleanup> generateMigrateRenameColumn(RenameColumn migration, String database, String table) {
        String query = String.format("ALTER TABLE %s CHANGE %s %s",
                escapeTable(database, table),
                escapeIdentifier(migration.getFromColumn()),
                escapeIdentifier(migration.getToColumn())
        );
        return Collections.singletonList(new QueryWithCleanup(query, null, null));
    }

    static List<QueryWithCleanup> generateMigrateCopyColumn(CopyColumn migration,
                                                            String database,
                                                            String table,
                                                            Column c) {
        String fromColumn = migration.getFromColumn();
        String toColumn = migration.getToColumn();

        String addColumnQuery = String.format("ALTER TABLE %s ADD COLUMN %s %s",
                escapeTable(database, table),
                escapeIdentifier(toColumn),
                mapDataTypes(c.getType(), c.getParams())
        );
        String copyDataQuery = String.format("UPDATE %s SET %s = %s",
                escapeTable(database, table),
                escapeIdentifier(toColumn),
                escapeIdentifier(fromColumn)
        );
        String dropColumnQuery = String.format("ALTER TABLE %s DROP COLUMN %s",
                escapeTable(database, table),
                escapeIdentifier(toColumn)
        );

        return Arrays.asList(new QueryWithCleanup(addColumnQuery, null, null),
                new QueryWithCleanup(copyDataQuery, dropColumnQuery, null));
    }

    static List<QueryWithCleanup> generateMigrateCopyTable(String tableFrom, String tableTo, String database) {
        String query = String.format("CREATE TABLE %s AS SELECT * FROM %s", escapeTable(database, tableTo), escapeTable(database, tableFrom));
        return Collections.singletonList(new QueryWithCleanup(query, null, null));
    }

    static List<QueryWithCleanup> generateMigrateUpdateColumnValueOperation(UpdateColumnValueOperation migration, String database, String table, DataType type) {
        String sql = String.format("UPDATE %s SET %s = ?",
                escapeTable(database, table),
                escapeIdentifier(migration.getColumn()));

        QueryWithCleanup query = new QueryWithCleanup(sql, null, null);
        query.addParameter(migration.getValue(), type);
        return Collections.singletonList(query);
    }

    static List<QueryWithCleanup> generateMigrateLiveToSoftDelete(String database,
                                                                  String table,
                                                                  String softDeleteColumn) {
        String addColumnQuery = String.format("ALTER TABLE %s ADD COLUMN %s BOOLEAN",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn)
        );
        String copyDataQuery = String.format("UPDATE %s SET %s = FALSE WHERE %s IS NULL",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn),
                escapeIdentifier(softDeleteColumn)
        );
        String dropColumnQuery = String.format("ALTER TABLE %s DROP COLUMN %s",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn)
        );

        return Arrays.asList(new QueryWithCleanup(addColumnQuery, null, null),
                new QueryWithCleanup(copyDataQuery, dropColumnQuery, null));
    }

    static List<QueryWithCleanup> generateMigrateLiveToHistory(Table t,
                                                               String database,
                                                               String table) {
        // SingleStore doesn't support adding PK columns, so the table needs to be recreated from scratch.
        String tempTableName = getTempName(table);
        Table tempTable = t.toBuilder()
                .setName(tempTableName)
                .addColumns(
                        Column.newBuilder()
                                .setName("_fivetran_start")
                                .setType(DataType.NAIVE_DATETIME)
                                .setPrimaryKey(true)
                )
                .addColumns(
                        Column.newBuilder()
                                .setName("_fivetran_end")
                                .setType(DataType.NAIVE_DATETIME)
                )
                .addColumns(
                        Column.newBuilder()
                                .setName("_fivetran_active")
                                .setType(DataType.BOOLEAN)
                ).build();
        String createTableQuery = generateCreateTableQuery(database, tempTableName, tempTable);
        String populateDataQuery = String.format("INSERT INTO %s SELECT *, NOW() AS `_fivetran_start`, '9999-12-31 23:59:59.999999' AS `_fivetran_end`, TRUE AS `_fivetran_active` FROM %s",
                escapeTable(database, tempTableName), escapeTable(database, table));
        String dropTableQuery = String.format("DROP TABLE IF EXISTS %s", escapeTable(database, table));
        String renameTableQuery = String.format("ALTER TABLE %s RENAME %s", escapeTable(database, tempTableName), escapeIdentifier(table));

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(dropTableQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(renameTableQuery, null,
                        String.format("Failed to migrate table %s to history mode. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, table),
                                escapeTable(database, tempTableName),
                                escapeTable(database, tempTableName),
                                escapeTable(database, table)))
        );
    }

    static List<QueryWithCleanup> generateMigrateSoftDeleteToHistory(Table t,
                                                                     String database,
                                                                     String table,
                                                                     String softDeleteColumn) {
        // SingleStore doesn't support adding PK columns, so the table needs to be recreated from scratch.
        List<Column> tempTableColumns = t.getColumnsList().stream()
            .filter(c -> !c.getName().equals(softDeleteColumn))
            .collect(Collectors.toList());
        tempTableColumns.add(
            Column.newBuilder()
                .setName("_fivetran_start")
                .setType(DataType.NAIVE_DATETIME)
                .setPrimaryKey(true)
                .build()
        );
        tempTableColumns.add(
            Column.newBuilder()
                .setName("_fivetran_end")
                .setType(DataType.NAIVE_DATETIME)
                .build()
        );
        tempTableColumns.add(
            Column.newBuilder()
                .setName("_fivetran_active")
                .setType(DataType.BOOLEAN)
                .build()
        );

        String tempTableName = getTempName(table);
        Table tempTable = Table.newBuilder()
            .setName(tempTableName)
            .addAllColumns(tempTableColumns)
            .build();
        String createTableQuery = generateCreateTableQuery(database, tempTableName, tempTable);
        String populateDataQuery = String.format("INSERT INTO %s " +
                        "WITH _last_sync AS (SELECT MAX(_fivetran_synced) AS _last_sync FROM %s)" +
                        "SELECT %s, " +
                        "IF(%s, '1000-01-01 00:00:00.000000', (SELECT _last_sync FROM _last_sync)) AS `_fivetran_start`, " +
                        "IF(%s, '1000-01-01 00:00:00.000000', '9999-12-31 23:59:59.999999') AS `_fivetran_end`, " +
                        "IF(%s, FALSE, TRUE) AS `_fivetran_active` " +
                        "FROM %s",
                escapeTable(database, tempTableName),
                escapeTable(database, table),
                t.getColumnsList().stream()
                    .filter(c -> !c.getName().equals(softDeleteColumn))
                    .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                escapeIdentifier(softDeleteColumn),
                escapeIdentifier(softDeleteColumn),
                escapeIdentifier(softDeleteColumn),
                escapeTable(database, table)
        );

        String dropTableQuery = String.format("DROP TABLE IF EXISTS %s", escapeTable(database, table));
        String renameTableQuery = String.format("ALTER TABLE %s RENAME %s", escapeTable(database, tempTableName), escapeIdentifier(table));

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(dropTableQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(renameTableQuery, null,
                        String.format("Failed to migrate table %s to soft delete mode. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, table),
                                escapeTable(database, tempTableName),
                                escapeTable(database, tempTableName),
                                escapeTable(database, table)))
        );
    }

    static List<QueryWithCleanup> generateMigrateSoftDeleteToLive(String database,
                                                                  String table,
                                                                  String softDeleteColumn) {
        String deleteRows = String.format("DELETE FROM %s WHERE %s",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn)
        );
        String dropColumnQuery = String.format("ALTER TABLE %s DROP COLUMN %s",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn)
        );

        return Arrays.asList(new QueryWithCleanup(deleteRows, null, null),
                new QueryWithCleanup(dropColumnQuery, null, null));
    }

    static List<QueryWithCleanup> generateMigrateHistoryToSoftDelete(Table t,
                                                                     String database,
                                                                     String table,
                                                                     String softDeletedColumn) {
        // SingleStore doesn't support adding PK columns, so the table needs to be recreated from scratch.
        String tempTableName = getTempName(table);
        List<Column> tempTableColumns = t.getColumnsList().stream()
                .filter(c ->
                        !c.getName().equals("_fivetran_start") &&
                                !c.getName().equals("_fivetran_end") &&
                                !c.getName().equals("_fivetran_active")
                )
                .collect(Collectors.toList());
        tempTableColumns.add(Column.newBuilder()
                .setName(softDeletedColumn)
                .setType(DataType.BOOLEAN)
                .build()
        );

        Table tempTable = Table.newBuilder()
                .setName(tempTableName)
                .addAllColumns(tempTableColumns).build();

        String createTableQuery = generateCreateTableQuery(database, tempTableName, tempTable);
        String populateDataQuery;
        if (tempTableColumns.stream().noneMatch(Column::getPrimaryKey)) {
            populateDataQuery = String.format("INSERT INTO %s " +
                            "SELECT %s, IF(_fivetran_active, FALSE, TRUE) AS %s" +
                            "FROM %s",
                    escapeTable(database, tempTableName),
                    tempTableColumns.stream().filter(c -> !c.getName().equals(softDeletedColumn))
                            .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                    escapeIdentifier(softDeletedColumn),
                    escapeTable(database, table)
            );
        } else {
            populateDataQuery = String.format("INSERT INTO %s " +
                            "SELECT %s, IF(_fivetran_active, FALSE, TRUE) AS %s " +
                            "FROM (" +
                            " SELECT *, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY _fivetran_start DESC) as rn FROM %s" +
                            ") ranked " +
                            "WHERE rn = 1",
                    escapeTable(database, tempTableName),
                    tempTableColumns.stream().filter(c -> !c.getName().equals(softDeletedColumn))
                            .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                    escapeIdentifier(softDeletedColumn),
                    tempTableColumns.stream().filter(Column::getPrimaryKey)
                            .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                    escapeTable(database, table)
            );
        }
        String dropTableQuery = String.format("DROP TABLE IF EXISTS %s", escapeTable(database, table));
        String renameTableQuery = String.format("ALTER TABLE %s RENAME %s", escapeTable(database, tempTableName), escapeIdentifier(table));

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(dropTableQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(renameTableQuery, null,
                        String.format("Failed to migrate table %s to soft delete mode. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, table),
                                escapeTable(database, tempTableName),
                                escapeTable(database, tempTableName),
                                escapeTable(database, table)))
        );
    }

    static List<QueryWithCleanup> generateMigrateHistoryToLive(Table t,
                                                               String database,
                                                               String table,
                                                               Boolean keep_deleted_rows) {
        // SingleStore doesn't support adding PK columns, so the table needs to be recreated from scratch.
        String tempTableName = getTempName(table);
        Table tempTable = Table.newBuilder()
                .setName(tempTableName)
                .addAllColumns(
                        t.getColumnsList().stream()
                                .filter(c ->
                                        !c.getName().equals("_fivetran_start") &&
                                                !c.getName().equals("_fivetran_end") &&
                                                !c.getName().equals("_fivetran_active")
                                )
                                .collect(Collectors.toList())
                ).build();

        String createTableQuery = generateCreateTableQuery(database, tempTableName, tempTable);
        String populateDataQuery = String.format("INSERT INTO %s " +
                        "SELECT %s " +
                        "FROM %s" +
                        "%s",
                escapeTable(database, tempTableName),
                tempTable.getColumnsList().stream().map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                escapeTable(database, table),
                keep_deleted_rows ? "" : " WHERE _fivetran_active"
        );
        String dropTableQuery = String.format("DROP TABLE IF EXISTS %s", escapeTable(database, table));
        String renameTableQuery = String.format("ALTER TABLE %s RENAME %s", escapeTable(database, tempTableName), escapeIdentifier(table));

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(dropTableQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(renameTableQuery, null,
                        String.format("Failed to migrate table %s to live mode. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, table),
                                escapeTable(database, tempTableName),
                                escapeTable(database, tempTableName),
                                escapeTable(database, table)))
        );
    }

    static List<QueryWithCleanup> generateMigrateCopyTableToHistoryMode(Table t,
                                                                        String database,
                                                                        String fromTable,
                                                                        String toTable,
                                                                        String softDeleteColumn) {
        List<Column> newTableColumns = new ArrayList<>(t.getColumnsList());
        if (softDeleteColumn != null && !softDeleteColumn.isEmpty()) {
            newTableColumns = newTableColumns.stream()
                    .filter(c -> !c.getName().equals(softDeleteColumn))
                    .collect(Collectors.toList());
        }
        newTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_start")
                        .setType(DataType.NAIVE_DATETIME)
                        .setPrimaryKey(true)
                        .build()
        );
        newTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_end")
                        .setType(DataType.NAIVE_DATETIME)
                        .build()
        );
        newTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_active")
                        .setType(DataType.BOOLEAN)
                        .build()
        );

        Table newTable = Table.newBuilder()
                .setName(toTable)
                .addAllColumns(newTableColumns)
                .build();

        String createTableQuery = generateCreateTableQuery(database, toTable, newTable);
        String populateDataQuery;
        if (softDeleteColumn == null || softDeleteColumn.isEmpty()) {
            populateDataQuery = String.format("INSERT INTO %s " +
                            "SELECT *, " +
                            "NOW() AS `_fivetran_start`, " +
                            "'9999-12-31 23:59:59.999999' AS `_fivetran_end`, " +
                            "TRUE AS `_fivetran_active` " +
                            "FROM %s",
                    escapeTable(database, toTable),
                    escapeTable(database, fromTable)
            );

        } else {
            populateDataQuery = String.format("INSERT INTO %s " +
                            "WITH _last_sync AS (SELECT MAX(_fivetran_synced) AS _last_sync FROM %s)" +
                            "SELECT %s, " +
                            "IF(%s, '1000-01-01 00:00:00.000000', (SELECT _last_sync FROM _last_sync)) AS `_fivetran_start`, " +
                            "IF(%s, '1000-01-01 00:00:00.000000', '9999-12-31 23:59:59.999999') AS `_fivetran_end`, " +
                            "IF(%s, FALSE, TRUE) AS `_fivetran_active` " +
                            "FROM %s",
                    escapeTable(database, toTable),
                    escapeTable(database, fromTable),
                    t.getColumnsList().stream()
                            .filter(c -> !c.getName().equals(softDeleteColumn))
                            .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                    escapeIdentifier(softDeleteColumn),
                    escapeIdentifier(softDeleteColumn),
                    escapeIdentifier(softDeleteColumn),
                    escapeTable(database, fromTable)
            );
        }

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE IF EXISTS %s", escapeTable(database, toTable)), null)
        );
    }

    static List<QueryWithCleanup> generateDropColumnInHistoryMode(DropColumnInHistoryMode migration, Table t, String database, String table) {
        String column = migration.getColumn();
        String operationTimestamp = migration.getOperationTimestamp();

        QueryWithCleanup insertQuery = new QueryWithCleanup(String.format("INSERT INTO %s (%s, %s, _fivetran_start) " +
                        "SELECT %s, NULL as %s, ? AS _fivetran_start " +
                        "FROM %s " +
                        "WHERE _fivetran_active AND %s IS NOT NULL AND _fivetran_start < ?",
                escapeTable(database, table),
                t.getColumnsList().stream()
                        .filter(c -> !c.getName().equals(column) && !c.getName().equals("_fivetran_start"))
                        .map(c -> escapeIdentifier(c.getName()))
                        .collect(Collectors.joining(", ")),
                escapeIdentifier(column),
                t.getColumnsList().stream()
                        .filter(c -> !c.getName().equals(column) && !c.getName().equals("_fivetran_start"))
                        .map(c -> escapeIdentifier(c.getName()))
                        .collect(Collectors.joining(", ")),
                escapeIdentifier(column),
                escapeTable(database, table),
                escapeIdentifier(column)
        ), null, null)
            .addParameter(operationTimestamp, DataType.NAIVE_DATETIME)
            .addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        QueryWithCleanup updateQuery = new QueryWithCleanup(String.format("UPDATE %s " +
                        "SET _fivetran_end = DATE_SUB(?,  INTERVAL 1 MICROSECOND), _fivetran_active = FALSE " +
                        "WHERE _fivetran_active AND %s IS NOT NULL AND _fivetran_start < ?",
                escapeTable(database, table),
                escapeIdentifier(column)
        ), null, null);
        updateQuery.addParameter(operationTimestamp, DataType.NAIVE_DATETIME);
        updateQuery.addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        return Arrays.asList(insertQuery, updateQuery);
    }

    static List<QueryWithCleanup> generateAddColumnInHistoryMode(AddColumnInHistoryMode migration, Table t, String database, String table, boolean isEmptyTable) {
        String column = migration.getColumn();
        DataType columnType = migration.getColumnType();
        String defaultValue = migration.getDefaultValue();
        String operationTimestamp = migration.getOperationTimestamp();

        QueryWithCleanup alterTableQuery = new QueryWithCleanup(
            String.format("ALTER TABLE %s ADD COLUMN %s %s",
                escapeTable(database, table),
                escapeIdentifier(column),
                JDBCUtil.mapDataTypes(columnType, null)
            ),
            null, null);

        if (isEmptyTable) {
            return Collections.singletonList(alterTableQuery);
        }

        String dropColumnCleanup = String.format("ALTER TABLE %s DROP COLUMN %s", escapeTable(database, table), escapeIdentifier(column));

        QueryWithCleanup insertQuery = new QueryWithCleanup(String.format("INSERT INTO %s (%s, %s, _fivetran_start) " +
                "SELECT %s, ? :> %s as %s, ? AS _fivetran_start " +
                "FROM %s " +
                "WHERE _fivetran_active AND _fivetran_start < ?",
            escapeTable(database, table),
            t.getColumnsList().stream()
                .filter(c -> !c.getName().equals(column) && !c.getName().equals("_fivetran_start"))
                .map(c -> escapeIdentifier(c.getName()))
                .collect(Collectors.joining(", ")),
            escapeIdentifier(column),
            t.getColumnsList().stream()
                .filter(c -> !c.getName().equals(column) && !c.getName().equals("_fivetran_start"))
                .map(c -> escapeIdentifier(c.getName()))
                .collect(Collectors.joining(", ")),
            JDBCUtil.mapDataTypes(columnType, null),
            escapeIdentifier(column),
            escapeTable(database, table)
        ), dropColumnCleanup, null)
            .addParameter(defaultValue, columnType)
            .addParameter(operationTimestamp, DataType.NAIVE_DATETIME)
            .addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        QueryWithCleanup updateQuery = new QueryWithCleanup(String.format("UPDATE %s " +
                "SET _fivetran_end = DATE_SUB(?,  INTERVAL 1 MICROSECOND), _fivetran_active = FALSE " +
                "WHERE _fivetran_active AND _fivetran_start < ?",
            escapeTable(database, table)
        ), dropColumnCleanup, null);
        updateQuery.addParameter(operationTimestamp, DataType.NAIVE_DATETIME);
        updateQuery.addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        return Arrays.asList(alterTableQuery, insertQuery, updateQuery);
    }
}
