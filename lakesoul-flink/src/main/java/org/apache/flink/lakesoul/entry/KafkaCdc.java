/*
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.flink.lakesoul.entry;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulKafkaSinkOptions.*;

public class KafkaCdc {

    /**
     * @param args
     * --bootstrap_servers localhost:9092 --topic t_test --auto_offset_reset earliest --group_id test
     * --source.parallelism 4 --sink.parallelism 4 --job.checkpoint_interval 5000
     * --warehouse_path /tmp/lakesoul/kafka
     * --flink.checkpoint /tmp/lakesoul/chk
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String kafkaServers = parameter.get(BOOTSTRAP_SERVERS.key());
        String kafkaTopic = parameter.get(TOPIC.key());
        String topicGroupID = parameter.get(GROUP_ID.key());

        String topicOffset = parameter.get(START_OFFSET.key(), START_OFFSET.defaultValue());
        String startTimeStamp = parameter.get(START_TIMESTAMP.key());

        String databasePrefixPath = parameter.get(WAREHOUSE_PATH.key());
        int maxPollRecords = parameter.getInt(MAX_POLL_RECORDS.key(), MAX_POLL_RECORDS.defaultValue());
        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key(), 1);
        int sinkParallelism = parameter.getInt(BUCKET_PARALLELISM.key(), 1);
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(), JOB_CHECKPOINT_INTERVAL.defaultValue());
        boolean logicallyDropColumn = parameter.getBoolean(LOGICALLY_DROP_COLUM.key(), true);
        String serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        String schemaRegistryUrl = parameter.get(SCHEMA_REGISTRY_URL.key(), SCHEMA_REGISTRY_URL.defaultValue());
        boolean regularTopicName = parameter.getBoolean(REGULAR_TOPIC_NAME.key(), REGULAR_TOPIC_NAME.defaultValue());
        boolean kafkaDataAvroType = parameter.getBoolean(KAFKA_DATA_AVRO_TYPE.key(), KAFKA_DATA_AVRO_TYPE.defaultValue());
        String dbName = parameter.get(DBNAME.key(), DBNAME.defaultValue());

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
        pro.put("max.poll.records", maxPollRecords);
        pro.put("retries", 3);
        if (kafkaDataAvroType) {
            if (StringUtils.isEmpty(schemaRegistryUrl.trim())) {
                throw new IllegalArgumentException("Kafka Data Type is Avro, Schema Registry Url must be Required");
            }
            pro.put("schema.registry.url", schemaRegistryUrl);
            pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        }

        OffsetsInitializer offSet;
        switch (topicOffset) {
            case "latest":
                offSet = OffsetsInitializer.latest();
                break;
            case "earliest":
                offSet = OffsetsInitializer.earliest();
                break;
            case "timestamp":
                long startTimeMilli = LocalDateTime.parse(startTimeStamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .atZone(ZoneId.of(serverTimezone)).toInstant().toEpochMilli();
                offSet = OffsetsInitializer.timestamp(startTimeMilli);
                break;
            case "committedOffsets":
            default:
                offSet = OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
        }
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
        conf.set(LakeSoulSinkOptions.USE_CDC, true);
        conf.set(LakeSoulSinkOptions.isMultiTableSource, true);
        conf.set(LakeSoulSinkOptions.WAREHOUSE_PATH, databasePrefixPath);
        conf.set(LakeSoulSinkOptions.SOURCE_PARALLELISM, sourceParallelism);
        conf.set(LakeSoulSinkOptions.BUCKET_PARALLELISM, sinkParallelism);
        conf.set(LakeSoulSinkOptions.LOGICALLY_DROP_COLUM, logicallyDropColumn);
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.getConfig().registerTypeWithKryoSerializer(BinarySourceRecord.class, BinarySourceRecordSerializer.class);
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
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointStorage(parameter.get(FLINK_CHECKPOINT.key()));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // max failures per interval
                Time.of(10, TimeUnit.MINUTES), //time interval for measuring failure rate
                Time.of(20, TimeUnit.SECONDS) // delay
        ));

        LakeSoulRecordConvert lakeSoulRecordConvert = new LakeSoulRecordConvert(conf, serverTimezone);

        KafkaSourceBuilder<BinarySourceRecord> binarySourceRecordKafkaSourceBuilder = KafkaSource.<BinarySourceRecord>builder()
                .setProperties(pro)
                .setStartingOffsets(offSet);
        if (regularTopicName) {
            Pattern topicPattern = Pattern.compile(kafkaTopic);
            binarySourceRecordKafkaSourceBuilder.setTopicPattern(topicPattern);
        } else {
            binarySourceRecordKafkaSourceBuilder.setTopics(Arrays.asList(kafkaTopic.split(",")));
        }
        if (kafkaDataAvroType) {
            binarySourceRecordKafkaSourceBuilder.setDeserializer(new BinaryKafkaAvroRecordDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH), pro, dbName));
        } else {
            binarySourceRecordKafkaSourceBuilder.setDeserializer(new BinaryKafkaRecordDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH), dbName));
        }

        KafkaSource<BinarySourceRecord> kafkaSource = binarySourceRecordKafkaSourceBuilder.build();

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = conf;
        LakeSoulMultiTableSinkStreamBuilder builder = new LakeSoulMultiTableSinkStreamBuilder(kafkaSource, context, lakeSoulRecordConvert);
        DataStreamSource<BinarySourceRecord> source = builder.buildMultiTableSource("Kafka Source");

        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(source);
        DataStreamSink<BinarySourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        env.execute("LakeSoul CDC Sink From Kafka topic " + kafkaTopic);
    }
}