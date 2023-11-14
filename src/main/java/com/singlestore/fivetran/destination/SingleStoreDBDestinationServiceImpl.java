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

        try (Connection conn = JDBCUtil.createConnection(conf)) {
            DatabaseMetaData metadata=conn.getMetaData();

            try (ResultSet tables = metadata.getTables(database, null, table, null)) {
                if (!tables.next()) {
                    DescribeTableResponse response = DescribeTableResponse.newBuilder()
                            .setNotFound(true)
                            .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                // TODO: handle case when several tables are returned
            }

            Set<String> primaryKeys = new HashSet<>();
            try (ResultSet primaryKeysRS = metadata.getPrimaryKeys(database, null, table)) {
                primaryKeys.add(primaryKeysRS.getString("COLUMN_NAME"));
            }

            List<Column> columns = new ArrayList<>();
            try (ResultSet columnsRS = metadata.getColumns(database, null, table, null)) {
                while(columnsRS.next()) {
                    columns.add(Column.newBuilder()
                            .setName(columnsRS.getString("COLUMN_NAME"))
                            .setType(JDBCUtil.mapDataTypes(columnsRS.getInt("DATA_TYPE"), columnsRS.getString("TYPE_NAME")))
                            .setPrimaryKey(primaryKeys.contains(columnsRS.getString("COLUMN_NAME")))
                            // TODO: get scale and precision
                            .build());
                }
            }

            DescribeTableResponse response = DescribeTableResponse.newBuilder()
                    .setTable(
                            Table.newBuilder()
                                    .setName(request.getTableName())
                                    .addAllColumns(columns)
                                    .build())
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

        try (Connection conn = JDBCUtil.createConnection(conf)) {
            // TODO: implement
        } catch (SQLException e) {
            // TODO: handle
        }
        System.out.println("[CreateTable]: " + request.getSchemaName() + " | " + request.getTable().getName());
        responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void alterTable(AlterTableRequest request, StreamObserver<AlterTableResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();

        System.out.println("[AlterTable]: " + request.getSchemaName() + " | " + request.getTable().getName());
        responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void truncate(TruncateRequest request, StreamObserver<TruncateResponse> responseObserver) {
        System.out.println("[TruncateTable]: " + request.getSchemaName() + " | " + request.getTableName());
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

    private void createConnection(Map<String, String> configuration) {
        String host = configuration.get("host");
        String port = configuration.get("port");
        String user = configuration.get("user");
        String password = configuration.get("password");

    }
}