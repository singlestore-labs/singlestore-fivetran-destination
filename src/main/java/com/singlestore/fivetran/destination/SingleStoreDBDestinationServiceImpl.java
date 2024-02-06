package com.singlestore.fivetran.destination;

import com.singlestore.fivetran.destination.writers.DeleteWriter;
import com.singlestore.fivetran.destination.writers.LoadDataWriter;
import com.singlestore.fivetran.destination.writers.UpdateWriter;
import fivetran_sdk.*;
import io.grpc.stub.StreamObserver;

import java.sql.*;
import java.util.*;

public class SingleStoreDBDestinationServiceImpl extends DestinationGrpc.DestinationImplBase {
    @Override
    public void configurationForm(ConfigurationFormRequest request, StreamObserver<ConfigurationFormResponse> responseObserver) {
        // TODO: PLAT-6891 add more configurations
        responseObserver.onNext(
                ConfigurationFormResponse.newBuilder()
                    .setSchemaSelectionSupported(true)
                    .setTableSelectionSupported(true)
                    .addAllFields(Arrays.asList(
                        FormField.newBuilder()
                                .setName("host")
                                .setLabel("Host")
                                .setRequired(true)
                                .setTextField(TextField.PlainText)
                                .build(),
                        FormField.newBuilder()
                                .setName("port")
                                .setLabel("Port")
                                .setRequired(true)
                                .setTextField(TextField.PlainText)
                                .build(),
                        FormField.newBuilder()
                                .setName("user")
                                .setLabel("Username")
                                .setRequired(false)
                                .setTextField(TextField.PlainText)
                                .build(),
                        FormField.newBuilder()
                                .setName("password")
                                .setLabel("Password")
                                .setRequired(false)
                                .setTextField(TextField.Password)
                                .build(),
                        FormField.newBuilder()
                                .setName("ssl.mode")
                                .setLabel("SSL mode")
                                .setRequired(false)
                                .setDescription("Whether to use an encrypted connection to SingleStore. Options include: " +
                                    "'disable' to use an unencrypted connection (the default); " +
                                    "'trust' to use a secure (encrypted) connection (no certificate and hostname validation); " +
                                    "'verify_ca' to use a secure (encrypted) connection but additionally verify the server TLS certificate against the configured Certificate Authority " +
                                    "(CA) certificates, or fail if no valid matching CA certificates are found; or" +
                                    "'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.")
                                .setDropdownField(DropdownField.newBuilder()
                                    .addDropdownField("disable")
                                    .addDropdownField("trust")
                                    .addDropdownField("verify_ca")
                                    .addDropdownField("verify-full"))
                                .build(),
                        FormField.newBuilder()
                                .setName("ssl.keystore")
                                .setLabel("SSL Keystore")
                                .setRequired(false)
                                .setDescription("The location of the key store file. " + 
                                    "This is optional and can be used for two-way authentication between the client and the SingleStore Server.")
                                .setTextField(TextField.PlainText)
                                .build(),
                        FormField.newBuilder()
                                .setName("ssl.keystore.password")
                                .setLabel("SSL Keystore Password")
                                .setRequired(false)
                                .setDescription("The password for the key store file. " + 
                                    "This is optional and only needed if 'database.ssl.keystore' is configured.")
                                .setTextField(TextField.Password)
                                .build(),
                        FormField.newBuilder()
                                .setName("ssl.truststore")
                                .setLabel("SSL Truststore")
                                .setRequired(false)
                                .setDescription("The location of the trust store file for the server certificate verification.")
                                .setTextField(TextField.PlainText)
                                .build(),
                        FormField.newBuilder()
                                .setName("ssl.truststore.password")
                                .setLabel("SSL Truststore Password")
                                .setRequired(false)
                                .setDescription("The password for the trust store file. " + 
                                    "Used to check the integrity of the truststore, and unlock the truststore.")
                                .setTextField(TextField.Password)
                                .build(),
                        FormField.newBuilder()
                                .setName("ssl.server.cert")
                                .setLabel("SSL Server's Certificate")
                                .setRequired(false)
                                .setDescription("Server's certificate in DER format or the server's CA certificate. " +
                                    "The certificate is added to the trust store, which allows the connection to trust a self-signed certificate.")
                                .setTextField(TextField.PlainText)
                                .build(),
                        FormField.newBuilder()
                                .setName("driver.parameters")
                                .setLabel("Driver Parameters")
                                .setRequired(false)
                                .setDescription(
                                    "Additional JDBC parameters to use with connection string to SingleStore server.\n"+
                                    "Format: 'param1=value1; param2 = value2; ...'.\n"+ 
                                    "The supported parameters are available in the https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters .")
                                .setTextField(TextField.PlainText)
                                .build()
                    ))
                    .addAllTests(Collections.singletonList(
                        ConfigurationTest.newBuilder()
                            .setName("connect")
                            .setLabel("Tests connection")
                            .build()))
                    .build());

        responseObserver.onCompleted();
    }

    @Override
    public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
        String testName = request.getName();
        // TODO: PLAT-6892 make consistent logging
        System.out.println("test name: " + testName);

        if (testName.equals("connect")) {
            SingleStoreDBConfiguration configuration = new SingleStoreDBConfiguration(request.getConfigurationMap());
            try (Connection conn = JDBCUtil.createConnection(configuration);
                 Statement stmt = conn.createStatement();
            ) {
                stmt.execute("SELECT 1");
            } catch (Exception e) {
                responseObserver.onNext(TestResponse.newBuilder().setSuccess(false).setFailure(e.getMessage()).build());
                responseObserver.onCompleted();
                return;
            }
        }

        responseObserver.onNext(TestResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void describeTable(DescribeTableRequest request, StreamObserver<DescribeTableResponse> responseObserver) {
        // TODO: PLAT-6892 make consistent logging
        System.out.println("[DescribeTable]: " + request.getSchemaName() + "|" + request.getTableName());
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());
        String database = request.getSchemaName();
        String table = request.getTableName();

        try {
            Table t = JDBCUtil.getTable(conf, database, table);

            DescribeTableResponse response = DescribeTableResponse.newBuilder()
                    .setTable(t)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (JDBCUtil.TableNotExistException e) {
            DescribeTableResponse response = DescribeTableResponse.newBuilder()
                    .setNotFound(true)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            DescribeTableResponse response = DescribeTableResponse.newBuilder()
                    .setFailure(e.getMessage())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void createTable(CreateTableRequest request, StreamObserver<CreateTableResponse> responseObserver) {
        // TODO: PLAT-6892 make consistent logging
        System.out.println("[CreateTable]: " + request.getSchemaName() + "|" + request.getTable().getName());
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());

        String query = JDBCUtil.generateCreateTableQuery(request);
        try (
                Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()
        ) {
            stmt.execute(query);

            responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (SQLException e) {
            responseObserver.onNext(CreateTableResponse.newBuilder()
                    .setSuccess(false)
                    .setFailure(e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void alterTable(AlterTableRequest request, StreamObserver<AlterTableResponse> responseObserver) {
        // TODO: PLAT-6892 make consistent logging
        System.out.println("[AlterTable]: " + request.getSchemaName() + "|" + request.getTable().getName());
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());

        try (
                Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()
        ) {
            String query = JDBCUtil.generateAlterTableQuery(request);
            // query is null when table is not changed
            if (query != null) {
                stmt.execute(query);
            }

            responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (SQLException | JDBCUtil.TableNotExistException e) {
            responseObserver.onNext(AlterTableResponse.newBuilder()
                    .setSuccess(false)
                    .setFailure(e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void truncate(TruncateRequest request, StreamObserver<TruncateResponse> responseObserver) {
        // TODO: PLAT-6892 make consistent logging
        System.out.println("[Truncate]: " + request.getSchemaName() + "|" + request.getTableName());
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());

        try (
                Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()
        ) {
            String query = JDBCUtil.generateTruncateTableQuery(request);
            stmt.execute(query);

            responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (SQLException e) {
            responseObserver.onNext(TruncateResponse.newBuilder()
                    .setSuccess(false)
                    .setFailure(e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void writeBatch(WriteBatchRequest request, StreamObserver<WriteBatchResponse> responseObserver) {
        // TODO: PLAT-6892 make consistent logging
        System.out.println("[WriteBatch]: " + request.getSchemaName() + " | " + request.getTable().getName());
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());

        try (
                Connection conn = JDBCUtil.createConnection(conf);
        ) {
            LoadDataWriter w = new LoadDataWriter(conn, request.getSchemaName(), request.getTable(), request.getCsv(), request.getKeysMap());
            for (String file : request.getReplaceFilesList()) {
                System.out.println("Upsert file: " + file);
                w.write(file);
            }
            w.commit();

            UpdateWriter u = new UpdateWriter(conn, request.getSchemaName(), request.getTable(), request.getCsv(), request.getKeysMap());
            for (String file : request.getUpdateFilesList()) {
                System.out.println("Update file: " + file);
                u.write(file);
            }
            u.commit();


            DeleteWriter d = new DeleteWriter(conn, request.getSchemaName(), request.getTable(), request.getCsv(), request.getKeysMap());
            for (String file : request.getDeleteFilesList()) {
                System.out.println("Delete file: " + file);
                d.write(file);
            }
            d.commit();

            responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setSuccess(false)
                    .setFailure(e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }
}