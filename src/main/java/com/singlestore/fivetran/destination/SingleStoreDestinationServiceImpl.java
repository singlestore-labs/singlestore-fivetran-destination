package com.singlestore.fivetran.destination;

import com.singlestore.fivetran.destination.writers.DeleteWriter;
import com.singlestore.fivetran.destination.writers.LoadDataWriter;
import com.singlestore.fivetran.destination.writers.UpdateWriter;
import fivetran_sdk.*;
import io.grpc.stub.StreamObserver;

import java.sql.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleStoreDestinationServiceImpl extends DestinationGrpc.DestinationImplBase {
        private static final Logger logger =
                        LoggerFactory.getLogger(SingleStoreDestinationServiceImpl.class);

        @Override
        public void configurationForm(ConfigurationFormRequest request,
                        StreamObserver<ConfigurationFormResponse> responseObserver) {
                responseObserver.onNext(ConfigurationFormResponse.newBuilder()
                                .setSchemaSelectionSupported(true).setTableSelectionSupported(true)
                                .addAllFields(Arrays.asList(
                                                FormField.newBuilder().setName("host")
                                                                .setLabel("Host").setRequired(
                                                                                true)
                                                                .setTextField(TextField.PlainText)
                                                                .build(),
                                                FormField.newBuilder().setName("port")
                                                                .setLabel("Port").setRequired(true)
                                                                .setTextField(TextField.PlainText)
                                                                .build(),
                                                FormField.newBuilder().setName("user")
                                                                .setLabel("Username")
                                                                .setRequired(true)
                                                                .setTextField(TextField.PlainText)
                                                                .build(),
                                                FormField.newBuilder().setName("password")
                                                                .setLabel("Password")
                                                                .setRequired(false)
                                                                .setTextField(TextField.Password)
                                                                .build(),
                                                FormField.newBuilder().setName("ssl.mode")
                                                                .setLabel("SSL mode")
                                                                .setRequired(false)
                                                                .setDescription("Whether to use an encrypted connection to SingleStore. Options include: "
                                                                                + "'disable' to use an unencrypted connection (the default); "
                                                                                + "'trust' to use a secure (encrypted) connection (no certificate and hostname validation); "
                                                                                + "'verify_ca' to use a secure (encrypted) connection but additionally verify the server TLS certificate against the configured Certificate Authority "
                                                                                + "(CA) certificates, or fail if no valid matching CA certificates are found; or"
                                                                                + "'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.")
                                                                .setDropdownField(DropdownField
                                                                                .newBuilder()
                                                                                .addDropdownField(
                                                                                                "disable")
                                                                                .addDropdownField(
                                                                                                "trust")
                                                                                .addDropdownField(
                                                                                                "verify_ca")
                                                                                .addDropdownField(
                                                                                                "verify-full"))
                                                                .build(),
                                                FormField.newBuilder().setName("ssl.server.cert")
                                                                .setLabel("SSL Server's Certificate")
                                                                .setRequired(false)
                                                                .setDescription("Server's certificate in DER format or the server's CA certificate. "
                                                                                + "The certificate is added to the trust store, which allows the connection to trust a self-signed certificate.")
                                                                .setTextField(TextField.PlainText)
                                                                .build(),
                                                FormField.newBuilder().setName("driver.parameters")
                                                                .setLabel("Driver Parameters")
                                                                .setRequired(false)
                                                                .setDescription("Additional JDBC parameters to use with connection string to SingleStore server.\n"
                                                                                + "Format: 'param1=value1; param2 = value2; ...'.\n"
                                                                                + "The supported parameters are available in the https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters .")
                                                                .setTextField(TextField.PlainText)
                                                                .build()))
                                .addAllTests(Collections.singletonList(ConfigurationTest
                                                .newBuilder().setName("connect")
                                                .setLabel("Tests connection").build()))
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

                                responseObserver.onNext(TestResponse.newBuilder().setSuccess(false)
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
                SingleStoreConfiguration conf =
                                new SingleStoreConfiguration(request.getConfigurationMap());
                String database = request.getSchemaName();
                String table = request.getTableName();

                try {
                        Table t = JDBCUtil.getTable(conf, database, table);

                        DescribeTableResponse response =
                                        DescribeTableResponse.newBuilder().setTable(t).build();

                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                } catch (JDBCUtil.TableNotExistException e) {
                        logger.warn(String.format("Table %s doesn't exist",
                                        JDBCUtil.escapeTable(database, table)));

                        DescribeTableResponse response = DescribeTableResponse.newBuilder()
                                        .setNotFound(true).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                } catch (Exception e) {
                        logger.warn(String.format("DescribeTable failed for %s",
                                        JDBCUtil.escapeTable(database, table)), e);

                        DescribeTableResponse response = DescribeTableResponse.newBuilder()
                                        .setFailure(e.getMessage()).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                }
        }

        @Override
        public void createTable(CreateTableRequest request,
                        StreamObserver<CreateTableResponse> responseObserver) {
                SingleStoreConfiguration conf =
                                new SingleStoreConfiguration(request.getConfigurationMap());

                String query = JDBCUtil.generateCreateTableQuery(request);
                try (Connection conn = JDBCUtil.createConnection(conf);
                                Statement stmt = conn.createStatement()) {
                        logger.info(String.format("Executing SQL:\n %s", query));
                        stmt.execute(query);

                        responseObserver.onNext(
                                        CreateTableResponse.newBuilder().setSuccess(true).build());
                        responseObserver.onCompleted();
                } catch (Exception e) {
                        logger.warn(String.format("CreateTable failed for %s", JDBCUtil.escapeTable(
                                        request.getSchemaName(), request.getTable().getName())), e);

                        responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(false)
                                        .setFailure(e.getMessage()).build());
                        responseObserver.onCompleted();
                }
        }

        @Override
        public void alterTable(AlterTableRequest request,
                        StreamObserver<AlterTableResponse> responseObserver) {
                SingleStoreConfiguration conf =
                                new SingleStoreConfiguration(request.getConfigurationMap());

                try (Connection conn = JDBCUtil.createConnection(conf);
                                Statement stmt = conn.createStatement()) {
                        String query = JDBCUtil.generateAlterTableQuery(request);
                        // query is null when table is not changed
                        if (query != null) {
                                logger.info(String.format("Executing SQL:\n %s", query));
                                stmt.execute(query);
                        }

                        responseObserver.onNext(
                                        AlterTableResponse.newBuilder().setSuccess(true).build());
                        responseObserver.onCompleted();
                } catch (Exception e) {
                        logger.warn(String.format("AlterTable failed for %s", JDBCUtil.escapeTable(
                                        request.getSchemaName(), request.getTable().getName())), e);

                        responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(false)
                                        .setFailure(e.getMessage()).build());
                        responseObserver.onCompleted();
                }
        }

        @Override
        public void truncate(TruncateRequest request,
                        StreamObserver<TruncateResponse> responseObserver) {
                SingleStoreConfiguration conf =
                                new SingleStoreConfiguration(request.getConfigurationMap());

                try (Connection conn = JDBCUtil.createConnection(conf);
                                Statement stmt = conn.createStatement()) {
                        if (!JDBCUtil.checkTableExists(stmt, request.getSchemaName(),
                                        request.getTableName())) {
                                logger.warn(String.format("Table %s doesn't exist",
                                                JDBCUtil.escapeTable(request.getSchemaName(),
                                                                request.getTableName())));
                                responseObserver.onNext(TruncateResponse.newBuilder()
                                                .setSuccess(true).build());
                                responseObserver.onCompleted();
                                return;
                        }

                        String query = JDBCUtil.generateTruncateTableQuery(request);
                        logger.info(String.format("Executing SQL:\n %s", query));
                        stmt.execute(query);

                        responseObserver.onNext(
                                        TruncateResponse.newBuilder().setSuccess(true).build());
                        responseObserver.onCompleted();
                } catch (Exception e) {
                        if (e.getMessage().matches(""))
                                logger.warn(String.format("TruncateTable failed for %s",
                                                JDBCUtil.escapeTable(request.getSchemaName(),
                                                                request.getTableName())),
                                                e);

                        responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(false)
                                        .setFailure(e.getMessage()).build());
                        responseObserver.onCompleted();
                }
        }

        @Override
        public void writeBatch(WriteBatchRequest request,
                        StreamObserver<WriteBatchResponse> responseObserver) {
                SingleStoreConfiguration conf =
                                new SingleStoreConfiguration(request.getConfigurationMap());

                try (Connection conn = JDBCUtil.createConnection(conf);) {
                        LoadDataWriter w = new LoadDataWriter(conn, request.getSchemaName(),
                                        request.getTable(), request.getCsv(), request.getKeysMap());
                        for (String file : request.getReplaceFilesList()) {
                                w.write(file);
                        }
                        w.commit();

                        UpdateWriter u = new UpdateWriter(conn, request.getSchemaName(),
                                        request.getTable(), request.getCsv(), request.getKeysMap());
                        for (String file : request.getUpdateFilesList()) {
                                u.write(file);
                        }
                        u.commit();


                        DeleteWriter d = new DeleteWriter(conn, request.getSchemaName(),
                                        request.getTable(), request.getCsv(), request.getKeysMap());
                        for (String file : request.getDeleteFilesList()) {
                                d.write(file);
                        }
                        d.commit();

                        responseObserver.onNext(
                                        WriteBatchResponse.newBuilder().setSuccess(true).build());
                        responseObserver.onCompleted();
                } catch (Exception e) {
                        logger.warn(String.format("WriteBatch failed for %s", JDBCUtil.escapeTable(
                                        request.getSchemaName(), request.getTable().getName())), e);

                        responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(false)
                                        .setFailure(e.getMessage()).build());
                        responseObserver.onCompleted();
                }
        }
}
