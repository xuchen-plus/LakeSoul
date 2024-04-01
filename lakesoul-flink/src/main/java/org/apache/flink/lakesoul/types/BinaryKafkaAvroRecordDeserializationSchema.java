// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BinaryKafkaAvroRecordDeserializationSchema implements KafkaRecordDeserializationSchema<BinarySourceRecord> {
    LakeSoulRecordConvert convert;
    String basePath;
    private ObjectMapper objectMapper;

    private transient KafkaAvroDeserializer inner;

    private final String registryUrl;

    public BinaryKafkaAvroRecordDeserializationSchema(LakeSoulRecordConvert convert, String basePath, String registryUrl) {
        this.convert = convert;
        this.basePath = basePath;
        objectMapper = new ObjectMapper();
        this.registryUrl = registryUrl;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<BinarySourceRecord> collector) throws IOException {
        try {
            checkInitialized();
            String topic = consumerRecord.topic();

            byte[] key = consumerRecord.key();
            byte[] value = consumerRecord.value();

            GenericRecord keyRecord = (GenericRecord) inner.deserialize(topic, key);
            GenericRecord valueRecord = (GenericRecord) inner.deserialize(topic, value);

            BinarySourceRecord binarySourceRecord = BinarySourceRecord.fromKafkaAvroSourceRecord(consumerRecord, keyRecord, valueRecord, this.convert, this.basePath, objectMapper);
            collector.collect(binarySourceRecord);
        } catch (Exception e) {
            throw new IOException(String.format("Failed to deserialize consumer record %s.", e.getMessage()), e);
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(new TypeHint<BinarySourceRecord>() {});
    }

    private void checkInitialized() {
        if (inner == null) {
            Map<String, Object> props = new HashMap<>();
            props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
            SchemaRegistryClient client =
                    new CachedSchemaRegistryClient(
                            registryUrl, AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
            inner = new KafkaAvroDeserializer(client, props);
        }
    }

}
