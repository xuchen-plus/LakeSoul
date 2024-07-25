// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters.RowDataToAvroConverter;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.lakesoul.source.arrow.LakeSoulArrowSource;
import org.apache.flink.lakesoul.tool.LakeSoulKeyGen;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.LakSoulKafkaPartitioner;
import org.apache.flink.lakesoul.types.LakeSoulDynamicKafkaRecordSerializationSchema;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulKafkaSinkOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;

/**
 * Params arg:
 * --bootstrap_servers localhost:9092 --topic test --group_id test_id
 * --dbname source_test_db --table_name source_table --schema_registry_url http://localhost:8081
 * --source.parallelism 1 --sink.parallelism 1 --job.checkpoint_interval 5000
 * --flink.checkpoint file:///tmp/lakesoul/chk
 */
public class LakeSoulKafkaAvroSink {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String kafkaServers = parameter.get(BOOTSTRAP_SERVERS.key());
        String kafkaTopic = parameter.get(TOPIC.key());
        String clientID = parameter.get(CLIENT_ID.key());
        String topicGroupID = parameter.get(GROUP_ID.key());

        String lakeSoulDBName = parameter.get(DBNAME.key());
        String lakeSoulTableName = parameter.get(TABLE_NAME.key());

        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key(), 1);
        int sinkParallelism = parameter.getInt(BUCKET_PARALLELISM.key(), 1);
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());
        String serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        String schemaRegistryUrl = parameter.get(SCHEMA_REGISTRY_URL.key(), SCHEMA_REGISTRY_URL.defaultValue());
        int maxPollRecords = parameter.getInt(MAX_POLL_RECORDS.key(), MAX_POLL_RECORDS.defaultValue());

        //about security
        String securityProtocol = parameter.get(SECURITY_PROTOCOL.key());
        String saslMechanism = parameter.get(SASL_MECHANISM.key());
        String saslJaasConfig = parameter.get(SASL_JAAS_CONFIG.key());
        String sslTrustStoreLocation = parameter.get(SSL_TRUSTSTORE_LOCATION.key());
        String sslTrustStorePasswd = parameter.get(SSL_TRUSTSTORE_PASSWORD.key());
        String sslKeyStoreLocation = parameter.get(SSL_KEYSTORE_LOCATION.key());
        String sslKeyStorePasswd = parameter.get(SSL_KEYSTORE_PASSWORD.key());

        Properties pro = new Properties();
        pro.put("bootstrap.servers", kafkaServers);
        pro.put("group.id", topicGroupID);
        pro.put("client.id", clientID);
        pro.put("max.poll.records", maxPollRecords);
        pro.put("retries", 3);
        pro.put("transaction.timeout.ms", "60000");

        if (securityProtocol != null) {
            pro.put("security.protocol", securityProtocol);
            if (securityProtocol.equals("SASL_PLAINTEXT")) {
                pro.put("sasl.mechanism", saslMechanism);
                pro.put("sasl.jaas.config", saslJaasConfig);
            } else if (securityProtocol.equals("SSL")) {
                // SSL configurations
                // Configure the path of truststore (CA) provided by the server
                pro.put("ssl.truststore.location", sslTrustStoreLocation);
                pro.put("ssl.truststore.password", sslTrustStorePasswd);
                // Configure the path of keystore (private key) if client authentication is required
                pro.put("ssl.keystore.location", sslKeyStoreLocation);
                pro.put("ssl.keystore.password", sslKeyStorePasswd);
                pro.put("ssl.endpoint.identification.algorithm", "");
            } else if (securityProtocol.equals("SASL_SSL")) {
                // SSL configurations
                // Configure the path of truststore (CA) provided by the server
                pro.put("ssl.truststore.location", sslTrustStoreLocation);
                pro.put("ssl.truststore.password", sslTrustStorePasswd);
                // Configure the path of keystore (private key) if client authentication is required
                pro.put("ssl.keystore.location", sslKeyStoreLocation);
                pro.put("ssl.keystore.password", sslKeyStorePasswd);
                // SASL configurations
                // Set SASL mechanism as SCRAM-SHA-256
                pro.put("sasl.mechanism", saslMechanism);
                // Set JAAS configurations
                pro.put("sasl.jaas.config", saslJaasConfig);
                pro.put("ssl.endpoint.identification.algorithm", "");
            }
        }

        Configuration conf = new Configuration();
        // parameters for mutil tables dml sink
        conf.set(INFERRING_SCHEMA, true);
        conf.set(LakeSoulSinkOptions.USE_CDC, true);
        conf.set(LakeSoulSinkOptions.SOURCE_PARALLELISM, sourceParallelism);
        conf.set(LakeSoulSinkOptions.BUCKET_PARALLELISM, sinkParallelism);
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);

        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if (parameter.get(JOB_CHECKPOINT_MODE.key(), JOB_CHECKPOINT_MODE.defaultValue()).equals("AT_LEAST_ONCE")) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointStorage(parameter.get(FLINK_CHECKPOINT.key()));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // max failures per interval
                Time.of(10, TimeUnit.MINUTES), //time interval for measuring failure rate
                Time.of(20, TimeUnit.SECONDS) // delay
        ));

        DataStreamSource<LakeSoulArrowWrapper> source = env.fromSource(
                LakeSoulArrowSource.create(
                        lakeSoulDBName,
                        lakeSoulTableName,
                        conf
                ),
                WatermarkStrategy.noWatermarks(),
                "LakeSoul Arrow Source"
        );


        source.setParallelism(sourceParallelism).setParallelism(sourceParallelism);

        Map<String, Object> props = new HashMap<String, Object>((Map) pro);

        KafkaSink<Tuple3<byte[], byte[], Long>> kafkaSink = KafkaSink.<Tuple3<byte[], byte[], Long>>builder()
                .setBootstrapServers(kafkaServers)
                .setRecordSerializer(new LakeSoulDynamicKafkaRecordSerializationSchema(kafkaTopic, new LakSoulKafkaPartitioner<>()))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setKafkaProducerConfig(pro)
                .build();


        Tuple4<ConfluentRegistryAvroSerializationSchema, RowDataToAvroConverter, RowType, RowData.FieldGetter[]>  keyInfo = getKeyInfo(lakeSoulDBName,
                lakeSoulTableName, kafkaTopic, schemaRegistryUrl, props);
        ConfluentRegistryAvroSerializationSchema keySerialization;
        RowDataToAvroConverter keyRowDataToAvroConverter;
        final RowType keyRowType;
        FieldGetter[] keyFieldGetters;
        if (keyInfo != null) {
            keySerialization = keyInfo.f0;
            keyRowDataToAvroConverter = keyInfo.f1 ;
            keyRowType = keyInfo.f2;
            keyFieldGetters = keyInfo.f3;
        } else {
            keySerialization = null;
            keyRowDataToAvroConverter = null;
            keyRowType = null;
            keyFieldGetters = null;
        }


        SingleOutputStreamOperator<Tuple3<byte[], byte[], Long>> sinkKafkaRecordSingleOutputStreamOperator = source.flatMap(
                new FlatMapFunction<LakeSoulArrowWrapper, Tuple3<byte[], byte[], Long>>() {
                    @Override
                    public void flatMap(LakeSoulArrowWrapper lakeSoulArrowWrapper, Collector<Tuple3<byte[], byte[], Long>> collector)
                            throws Exception {
                        TableSchemaIdentity tableSchemaIdentity = lakeSoulArrowWrapper.generateTableSchemaIdentity();
                        RowType rowType = tableSchemaIdentity.rowType;
                        boolean useCDC = tableSchemaIdentity.useCDC;
                        String cdcColumn = tableSchemaIdentity.cdcColumn;
                        RowType kafkaAvroType = toKafkaAvroType(rowType, useCDC, cdcColumn);

                        lakeSoulArrowWrapper.withDecoded(ArrowUtils.getRootAllocator(), (tableInfo, recordBatch) -> {
                            ArrowReader arrowReader = ArrowUtils.createArrowReader(recordBatch, rowType);
                            int i = 0;
                            while (i < recordBatch.getRowCount()) {
                                RowData rowData = arrowReader.read(i);
                                RowData kafkaRowData = toKafkaAvroRawData(rowData, rowType, String.format("%s.%s", lakeSoulDBName, lakeSoulTableName));

                                byte[] keyBytes = null;
                                if (keySerialization != null ) {
                                    GenericRecord keyGenericRecord = (GenericRecord) keyRowDataToAvroConverter.convert(
                                            AvroSchemaConverter.convertToSchema(keyRowType),
                                            createProjectedRow(rowData, RowKind.INSERT, keyFieldGetters));
                                    keyBytes = keySerialization.serialize(keyGenericRecord);
                                }

                                ConfluentRegistryAvroSerializationSchema<GenericRecord> genericRecordConfluentRegistryAvroSerializationSchema =
                                        ConfluentRegistryAvroSerializationSchema.forGeneric(
                                                String.format("%s-value", kafkaTopic),
                                                AvroSchemaConverter.convertToSchema(kafkaAvroType, false),
                                                schemaRegistryUrl,
                                                props);
                                RowDataToAvroConverter valueConverter = RowDataToAvroConverters.createConverter(kafkaAvroType, false);
                                GenericRecord valueGenericRecord = (GenericRecord) valueConverter.convert(AvroSchemaConverter.convertToSchema(kafkaAvroType, false), kafkaRowData);
                                byte[] valueBytes = genericRecordConfluentRegistryAvroSerializationSchema.serialize(valueGenericRecord);

                                long hash = 42;
                                List<String> primaryKeys = tableSchemaIdentity.primaryKeys;
                                for (String pk : primaryKeys) {
                                    int typeIndex = rowType.getFieldIndex(pk);
                                    LogicalType type = rowType.getTypeAt(typeIndex);
                                    Object fieldOrNull = RowData.createFieldGetter(type, typeIndex)
                                            .getFieldOrNull(rowData);
                                    hash = LakeSoulKeyGen.getHash(type, fieldOrNull, hash);
                                }

                                collector.collect(new Tuple3<>(keyBytes, valueBytes, hash));
                                i++;
                            }
                        });
                    }
                }).setParallelism(sourceParallelism);

        sinkKafkaRecordSingleOutputStreamOperator.sinkTo(kafkaSink).setParallelism(sinkParallelism);
        env.execute("LakeSoul CDC Sink From Kafka topic " + kafkaTopic);
    }

    public static Tuple4<ConfluentRegistryAvroSerializationSchema, RowDataToAvroConverter, RowType, RowData.FieldGetter[]> getKeyInfo(String dbName,
                                                                                                                                      String table, String topic, String schemaRegistryUrl, Map<String, ?> kafkaConfigs) {
        DBManager lakesoulDBManager = new DBManager();
        TableInfo tableInfo = lakesoulDBManager.getTableInfoByNameAndNamespace(table, dbName);
        String tableSchema = tableInfo.getTableSchema();
        DBUtil.TablePartitionKeys partitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
        List<String> primaryKeys = partitionKeys.primaryKeys;

        final RowData.FieldGetter[] keyFieldGetters;
        RowType keyRowType;
        int[] keyIndex;;

        if (primaryKeys.size() > 0) {
            try {
                Schema schema = Schema.fromJSON(tableSchema);
                List<String> columns = schema.getFields().stream().map(f -> f.getName()).collect(Collectors.toList());
                keyIndex = Arrays.stream(primaryKeys.toArray(new String[0])).mapToInt(columns::indexOf).toArray();
                keyRowType = ArrowUtils.tablePrimaryArrowSchema(schema, primaryKeys);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            keyFieldGetters =
                    IntStream.range(0, keyRowType.getFieldCount())
                            .mapToObj(
                                    i ->
                                            RowData.createFieldGetter(
                                                    keyRowType.getTypeAt(i), keyIndex[i]))
                            .toArray(RowData.FieldGetter[]::new);

            ConfluentRegistryAvroSerializationSchema<GenericRecord>
                    genericRecordConfluentRegistryAvroSerializationSchema =
                    ConfluentRegistryAvroSerializationSchema.forGeneric(
                            String.format("%s-key", topic),
                            AvroSchemaConverter.convertToSchema(keyRowType),
                            schemaRegistryUrl,
                            kafkaConfigs);
            RowDataToAvroConverter keyRowDataToAvroConverter = RowDataToAvroConverters.createConverter(keyRowType);
            return new Tuple4<>(genericRecordConfluentRegistryAvroSerializationSchema, keyRowDataToAvroConverter, keyRowType, keyFieldGetters);
        }
        return null;
    }

    public static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    public static RowType toKafkaAvroType(RowType rowType, boolean useCDC, String cdcColumn) {
        LogicalType[] colTypes = new LogicalType[rowType.getFieldCount() + 2];
        String[] colNames = new String[rowType.getFieldCount() + 2];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            String colName = rowType.getFieldNames().get(i);
            if (useCDC && colName.equals(cdcColumn)) {
                colNames[i] = "op_type";
            } else {
                colNames[i] = colName;
            }
            colTypes[i] = rowType.getTypeAt(i);
        }
        colNames[rowType.getFieldCount()] = "table";
        colTypes[rowType.getFieldCount()] = new VarCharType(false, Integer.MAX_VALUE);
        colNames[rowType.getFieldCount() + 1] = "op_ts";
        colTypes[rowType.getFieldCount() + 1] = new VarCharType(false, Integer.MAX_VALUE);
        return RowType.of(colTypes, colNames);
    }

    public static RowData toKafkaAvroRawData(RowData rowData, RowType rowType, String table) {

        RowData.FieldGetter[] fieldGetters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                i ->
                                        RowData.createFieldGetter(
                                                rowType.getTypeAt(i), i))
                        .toArray(RowData.FieldGetter[]::new);

        int newArity = rowData.getArity() + 2;
        GenericRowData kafkaRowData = new GenericRowData(newArity);
        for (int i = 0; i < newArity - 2; ++i) {
            kafkaRowData.setField(i, fieldGetters[i].getFieldOrNull(rowData));
        }
        kafkaRowData.setRowKind(rowData.getRowKind());
        kafkaRowData.setField(kafkaRowData.getArity() - 2, StringData.fromString(table));
        kafkaRowData.setField(kafkaRowData.getArity() - 1, StringData.fromString(String.valueOf(System.currentTimeMillis())));
        return kafkaRowData;
    }
}
