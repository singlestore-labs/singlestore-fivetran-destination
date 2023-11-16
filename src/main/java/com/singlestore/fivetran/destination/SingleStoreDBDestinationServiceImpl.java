package com.singlestore.fivetran.destination;

import fivetran_sdk.*;
import io.grpc.stub.StreamObserver;

import java.sql.*;
import java.util.*;

public class SingleStoreDBDestinationServiceImpl extends DestinationGrpc.DestinationImplBase {
    @Override
    public void configurationForm(ConfigurationFormRequest request, StreamObserver<ConfigurationFormResponse> responseObserver) {
        responseObserver.onNext(
                ConfigurationFormResponse.newBuilder()
                        .setSchemaSelectionSupported(true)
                        .setTableSelectionSupported(true)
                        .addAllFields(Arrays.asList(
                                FormField.newBuilder()
                                        .setName("host").setLabel("Host").setRequired(true).setTextField(TextField.PlainText).build(),
                                FormField.newBuilder()
                                        .setName("port").setLabel("Port").setRequired(true).setTextField(TextField.PlainText).build(),
                                FormField.newBuilder()
                                        .setName("user").setLabel("Username").setRequired(false).setTextField(TextField.PlainText).build(),
                                FormField.newBuilder()
                                        .setName("password").setLabel("Password").setRequired(false).setTextField(TextField.Password).build())
                        )
                        .addAllTests(Collections.singletonList(
                                ConfigurationTest.newBuilder().setName("connect").setLabel("Tests connection").build()))
                        .build());

        responseObserver.onCompleted();
    }

    @Override
    public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
        String testName = request.getName();
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
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());

        String query = JDBCUtil.generateCreateTableQuery(request);
        try (
                Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()
        ) {
            stmt.execute(query);
        } catch (SQLException e) {
            responseObserver.onNext(CreateTableResponse.newBuilder()
                    .setSuccess(false)
                    .setFailure(e.getMessage())
                    .build());
            responseObserver.onCompleted();

            return;
        }

        responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void alterTable(AlterTableRequest request, StreamObserver<AlterTableResponse> responseObserver) {
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());

        try (
                Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()
        ) {
            String query = JDBCUtil.generateAlterTableQuery(request);
            stmt.execute(query);
        } catch (Exception e) {
            responseObserver.onNext(AlterTableResponse.newBuilder()
                    .setSuccess(false)
                    .setFailure(e.getMessage())
                    .build());
            responseObserver.onCompleted();

            return;
        }

        responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void truncate(TruncateRequest request, StreamObserver<TruncateResponse> responseObserver) {
        SingleStoreDBConfiguration conf = new SingleStoreDBConfiguration(request.getConfigurationMap());

        try (
                Connection conn = JDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()
        ) {
            String query = JDBCUtil.generateTruncateTableQuery(request);
            stmt.execute(query);
        } catch (Exception e) {
            responseObserver.onNext(TruncateResponse.newBuilder()
                    .setSuccess(false)
                    .setFailure(e.getMessage())
                    .build());
            responseObserver.onCompleted();

            return;
        }

        responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void writeBatch(WriteBatchRequest request, StreamObserver<WriteBatchResponse> responseObserver) {
        System.out.println("[WriteBatch]: " + request.getSchemaName() + " | " + request.getTable().getName());
        for (String file : request.getReplaceFilesList()) {
            System.out.println("Replace files: " + file);
        }
        for (String file : request.getUpdateFilesList()) {
            System.out.println("Update files: " + file);
        }
        for (String file : request.getDeleteFilesList()) {
            System.out.println("Delete files: " + file);
        }
        responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }
}