// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class LakeSoulKafkaSinkOptions extends LakeSoulSinkOptions{

    public static final ConfigOption<String> BOOTSTRAP_SERVERS = ConfigOptions
            .key("bootstrap_servers")
            .stringType()
            .noDefaultValue()
            .withDescription("source kafka bootstrap servers");

    public static final ConfigOption<String> GROUP_ID = ConfigOptions
            .key("group_id")
            .stringType()
            .noDefaultValue()
            .withDescription("source kafka group ID");

    public static final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("source from or Sink to Topic Name");

    public static final ConfigOption<String> START_OFFSET = ConfigOptions
            .key("auto_offset_reset")
            .stringType()
            .defaultValue("latest")
            .withDescription("source consume from the offset");

    public static final ConfigOption<String> START_TIMESTAMP = ConfigOptions
            .key("start_timestamp")
            .stringType()
            .noDefaultValue()
            .withDescription("source consume from the timestamp");

    public static final ConfigOption<Integer> MAX_POLL_RECORDS = ConfigOptions
            .key("max_poll_records")
            .intType()
            .defaultValue(500)
            .withDescription("source poll max records");

    public static final ConfigOption<String> SECURITY_PROTOCOL = ConfigOptions
            .key("security_protocol")
            .stringType()
            .noDefaultValue()
            .withDescription("security protocol when connect kafka,includes SASL_PLAINTEXT, SSL and SASL_SSL");

    public static final ConfigOption<String> SASL_MECHANISM = ConfigOptions
            .key("sasl_mechanism")
            .stringType()
            .noDefaultValue()
            .withDescription("Used to specify the security protocol used when connecting to the Kafka cluster,which includes SCRAM-SHA-256 and PLAIN");

    public static final ConfigOption<String> SASL_JAAS_CONFIG = ConfigOptions
            .key("sasl_jaas_config")
            .stringType()
            .noDefaultValue()
            .withDescription("Set up the JAAS configuration, including information such as the user's username and password");

    public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION = ConfigOptions
            .key("ssl_truststore_location")
            .stringType()
            .noDefaultValue()
            .withDescription("Configure the path of truststore (CA) provided by the server");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD = ConfigOptions
            .key("ssl_truststore_password")
            .stringType()
            .noDefaultValue()
            .withDescription("Configure the passwd of truststore (CA) provided by the server");

    public static final ConfigOption<String> SSL_KEYSTORE_LOCATION = ConfigOptions
            .key("ssl_keystore_location")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("Configure the path of keystore (private key) if client authentication is required");

    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD = ConfigOptions
            .key("ssl_keystore_password")
            .stringType()
            .noDefaultValue()
            .withDescription("Configure the passwd of keystore (private key) if client authentication is required");


    public static final ConfigOption<String> SCHEMA_REGISTRY_URL = ConfigOptions
            .key("schema_registry_url")
            .stringType()
            .noDefaultValue()
            .withDescription("Configure the schema registry url if the kafka service is required");

    public static final ConfigOption<Boolean> KAFKA_DATA_AVRO_TYPE = ConfigOptions
            .key("kafka_data_avro_type")
            .booleanType()
            .defaultValue(true)
            .withDescription("Kafka Data Type is Avro");

    public static final ConfigOption<Boolean> REGULAR_TOPIC_NAME = ConfigOptions
            .key("regular_topic_name")
            .booleanType()
            .defaultValue(true)
            .withDescription("Kafka Topic Name is Regular Expression");

    public static final ConfigOption<String> DBNAME = ConfigOptions
            .key("dbname")
            .stringType()
            .defaultValue(null)
            .withDescription("Database for Data into or from LakeSoul");

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key("table_name")
            .stringType()
            .noDefaultValue()
            .withDescription("LakeSoul Table Name");
}