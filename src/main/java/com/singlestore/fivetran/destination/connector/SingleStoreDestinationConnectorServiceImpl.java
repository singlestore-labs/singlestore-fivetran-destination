package com.singlestore.fivetran.destination.connector;

import com.singlestore.fivetran.destination.connector.warning_util.AlterTableWarningHandler;
import com.singlestore.fivetran.destination.connector.warning_util.DescribeTableWarningHandler;
import com.singlestore.fivetran.destination.connector.warning_util.WriteBatchWarningHandler;
import com.singlestore.fivetran.destination.connector.writers.DeleteWriter;
import com.singlestore.fivetran.destination.connector.writers.LoadDataWriter;
import com.singlestore.fivetran.destination.connector.writers.UpdateWriter;
import fivetran_sdk.v2.*;
import io.grpc.stub.StreamObserver;

import java.sql.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleStoreDestinationConnectorServiceImpl extends DestinationConnectorGrpc.DestinationConnectorImplBase {
    private static final Logger logger =
            LoggerFactory.getLogger(SingleStoreDestinationConnectorServiceImpl.class);

    @Override
    public void configurationForm(ConfigurationFormRequest request,
                                  StreamObserver<ConfigurationFormResponse> responseObserver) {
        FormField serverCert = FormField.newBuilder().setName("ssl.server.cert")
                .setLabel("SSL Server's Certificate").setRequired(false)
                .setDescription(
                        "Server's certificate in DER format or the server's CA certificate. "
                                + "The certificate is added to the trust store, which allows the connection to trust a self-signed certificate.")
                .setTextField(TextField.PlainText)
                .build();

        responseObserver.onNext(ConfigurationFormResponse.newBuilder()
                .setSchemaSelectionSupported(true).setTableSelectionSupported(true)
                .addAllFields(Arrays.asList(
                        FormField.newBuilder().setName("host").setLabel("Host").setRequired(true)
                                .setTextField(TextField.PlainText).build(),
                        FormField.newBuilder().setName("port").setLabel("Port").setRequired(true)
                                .setTextField(TextField.PlainText).build(),
                        FormField.newBuilder().setName("database").setLabel("Database")
                                .setRequired(false)
                                .setDescription(
                                        "SingleStore database in which data should be written.\n"
                                                + "If this option is specified, data will be written to a single database.\n"
                                                + "Each table name will consist of Fivetran schema name and table name (For example: `<schema>__<table>`).\n"
                                                + "If this option is not specified, appropriate SingleStore database will be created for each schema.\n"
                                                + "'CREATE DATABASE' permissions are required in this case.")
                                .setTextField(TextField.PlainText).build(),
                        FormField.newBuilder().setName("user").setLabel("Username")
                                .setRequired(true).setTextField(TextField.PlainText).build(),
                        FormField.newBuilder().setName("password").setLabel("Password")
                                .setRequired(false).setTextField(TextField.Password).build(),
                        FormField.newBuilder().setName("ssl.mode").setLabel("SSL mode")
                                .setRequired(false)
                                .setDescription(
                                        "Whether to use an encrypted connection to SingleStore.\n"
                                                + "Options include:\n"
                                                + " * 'disable' to use an unencrypted connection (the default);\n"
                                                + " * 'trust' to use a secure (encrypted) connection (no certificate and hostname validation);\n"
                                                + " * 'verify-ca' to use a secure (encrypted) connection but additionally verify the server TLS certificate against the configured Certificate Authority "
                                                + "(CA) certificates, or fail if no valid matching CA certificates are found;\n"
                                                + " * 'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.")
                                .setDropdownField(DropdownField.newBuilder()
                                        .addDropdownField("disable")
                                        .addDropdownField("trust")
                                        .addDropdownField("verify-ca")
                                        .addDropdownField("verify-full"))
                                .build(),
                        FormField.newBuilder()
                                .setConditionalFields(
                                        ConditionalFields.newBuilder()
                                                .setCondition(VisibilityCondition.newBuilder()
                                                        .setConditionField("ssl.mode")
                                                        .setStringValue("trust")
                                                        .build()
                                                )
                                                .addAllFields(
                                                        Collections.singletonList(serverCert))
                                                .build()
                                ).build(),
                        FormField.newBuilder()
                                .setConditionalFields(
                                        ConditionalFields.newBuilder()
                                                .setCondition(VisibilityCondition.newBuilder()
                                                        .setConditionField("ssl.mode")
                                                        .setStringValue("verify-ca")
                                                        .build()
                                                )
                                                .addAllFields(
                                                        Collections.singletonList(serverCert))
                                                .build()
                                ).build(),
                        FormField.newBuilder()
                                .setConditionalFields(
                                        ConditionalFields.newBuilder()
                                                .setCondition(VisibilityCondition.newBuilder()
                                                        .setConditionField("ssl.mode")
                                                        .setStringValue("verify-full")
                                                        .build()
                                                )
                                                .addAllFields(
                                                        Collections.singletonList(serverCert))
                                                .build()
                                ).build(),
                        FormField.newBuilder().setName("driver.parameters")
                                .setLabel("Driver Parameters").setRequired(false)
                                .setDescription(
                                        "Additional JDBC parameters to use with connection string to SingleStore server.\n"
                                                + "Format: 'param1=value1; param2 = value2; ...'.\n"
                                                + "The supported parameters are available in the https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters .")
                                .setTextField(TextField.PlainText).build(),
                        FormField.newBuilder().setName("batch.size").setLabel("Batch Size")
                                .setRequired(false)
                                .setDescription(
                                        "Maximum number of rows that will be changed by a query. Default is 10000")
                                .setTextField(TextField.PlainText).build()))
                .addAllTests(Collections.singletonList(ConfigurationTest.newBuilder()
                        .setName("connect").setLabel("Tests connection").build()))
                .build());

        responseObserver.onCompleted();
    }

    @Override
    public void capabilities(CapabilitiesRequest request,
                             StreamObserver<CapabilitiesResponse> responseObserver) {
        responseObserver.onNext(CapabilitiesResponse
                .newBuilder()
                .setBatchFileFormat(BatchFileFormat.CSV)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
        String testName = request.getName();

        if (testName.equals("connect")) {
            SingleStoreConfiguration configuration =
                    new SingleStoreConfiguration(request.getConfigurationMap());
            try (Connection conn = JDBCUtil.createConnection(configuration);
                 Statement stmt = conn.createStatement();) {
                stmt.execute("SELECT 1");
            } catch (Exception e) {
                logger.warn("Test failed", e);

                responseObserver.onNext(TestResponse.newBuilder()
                        .setFailure(e.getMessage()).build());
                responseObserver.onCompleted();
                return;
            }
        }

        responseObserver.onNext(TestResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void describeTable(DescribeTableRequest request,
                              StreamObserver<DescribeTableResponse> responseObserver) {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(request.getConfigurationMap());
        String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = JDBCUtil.getTableName(conf, request.getSchemaName(), request.getTableName());

        try {
            Table t = JDBCUtil.getTable(conf, database, table, table, new DescribeTableWarningHandler(responseObserver));

            DescribeTableResponse response = DescribeTableResponse.newBuilder().setTable(t).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (JDBCUtil.TableNotExistException e) {
            logger.warn(
                    String.format("Table %s doesn't exist", JDBCUtil.escapeTable(database, table)));

            DescribeTableResponse response =
                    DescribeTableResponse.newBuilder().setNotFound(true).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.warn(String.format("DescribeTable failed for %s",
                    JDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(DescribeTableResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void createTable(CreateTableRequest request,
                            StreamObserver<CreateTableResponse> responseObserver) {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(request.getConfigurationMap());

        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            String query = JDBCUtil.generateCreateTableQuery(conf, stmt, request);
            logger.info(String.format("Executing SQL:\n %s", query));
            stmt.execute(query);

            responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
            String table = JDBCUtil.getTableName(conf, request.getSchemaName(),
                    request.getTable().getName());

            logger.warn(String.format("CreateTable failed for %s",
                    JDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(CreateTableResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onNext(CreateTableResponse.newBuilder()
                    .setSuccess(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void alterTable(AlterTableRequest request,
                           StreamObserver<AlterTableResponse> responseObserver) {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(request.getConfigurationMap());

        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            String query = JDBCUtil.generateAlterTableQuery(request, new AlterTableWarningHandler(responseObserver));
            // query is null when table is not changed
            if (query != null) {
                logger.info(String.format("Executing SQL:\n %s", query));
                stmt.execute(query);
            }

            responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
            String table = JDBCUtil.getTableName(conf, request.getSchemaName(),
                    request.getTable().getName());
            logger.warn(String.format("AlterTable failed for %s",
                    JDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(AlterTableResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onNext(AlterTableResponse.newBuilder()
                    .setSuccess(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void truncate(TruncateRequest request,
                         StreamObserver<TruncateResponse> responseObserver) {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(request.getConfigurationMap());
        String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = JDBCUtil.getTableName(conf, request.getSchemaName(), request.getTableName());

        try (Connection conn = JDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            if (!JDBCUtil.checkTableExists(stmt, database, table)) {
                logger.warn(String.format("Table %s doesn't exist",
                        JDBCUtil.escapeTable(database, table)));
                responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
                return;
            }

            String query = JDBCUtil.generateTruncateTableQuery(conf, request);
            logger.info(String.format("Executing SQL:\n %s", query));
            stmt.execute(query);

            responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.warn(String.format("TruncateTable failed for %s",
                    JDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(TruncateResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onNext(TruncateResponse.newBuilder()
                    .setSuccess(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void writeBatch(WriteBatchRequest request,
                           StreamObserver<WriteBatchResponse> responseObserver) {
        SingleStoreConfiguration conf = new SingleStoreConfiguration(request.getConfigurationMap());
        String database = JDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table =
                JDBCUtil.getTableName(conf, request.getSchemaName(), request.getTable().getName());

        try (Connection conn = JDBCUtil.createConnection(conf);) {
            if (request.getTable().getColumnsList().stream()
                    .noneMatch(column -> column.getPrimaryKey())) {
                throw new Exception("No primary key found");
            }

            LoadDataWriter w =
                    new LoadDataWriter(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize(),
                            new WriteBatchWarningHandler(responseObserver));
            for (String file : request.getReplaceFilesList()) {
                w.write(file);
            }

            UpdateWriter u =
                    new UpdateWriter(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize());
            for (String file : request.getUpdateFilesList()) {
                u.write(file);
            }


            DeleteWriter d =
                    new DeleteWriter(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize());
            for (String file : request.getDeleteFilesList()) {
                d.write(file);
            }

            responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.warn(String.format("WriteBatch failed for %s",
                    JDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setSuccess(false)
                    .build());
            responseObserver.onCompleted();
        }
    }
}
