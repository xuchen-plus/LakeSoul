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

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.BinaryKafkaRecordDeserializationSchema;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulKafkaSinkOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulKafkaSinkOptions.SOURCE_PARALLELISM;

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
        boolean logicallyDropColumn = parameter.getBoolean(LOGICALLY_DROP_COLUM.key(), false);
        String serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());

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

        OffsetsInitializer offSet;
        switch (topicOffset) {
            case "latest":
                offSet = OffsetsInitializer.latest();
                break;
            case "earliest":
                offSet = OffsetsInitializer.earliest();
                break;
            case "timestamp":
                offSet = OffsetsInitializer.timestamp((Long.parseLong(startTimeStamp)));
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


        String[] splitTopicArray = kafkaTopic.split("_");
        if (splitTopicArray.length < 2) {
            throw new Exception("topic name is not standardized format: t_{dbName/schemaName}_{tableName} ");
        }
        String namespace = splitTopicArray[1];
        DBManager lakeSoulDBManager = new DBManager();
        if (lakeSoulDBManager.getNamespaceByNamespace(namespace) == null) {
            lakeSoulDBManager.createNewNamespace(namespace, new JSONObject(), "");
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

        KafkaSource<BinarySourceRecord> kafkaSource = KafkaSource.<BinarySourceRecord>builder()
                .setTopics(kafkaTopic)
                .setProperties(pro)
                .setStartingOffsets(offSet)
                .setDeserializer(new BinaryKafkaRecordDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH)))
                .build();

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