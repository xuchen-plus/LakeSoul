// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class LakeSoulDynamicKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<Tuple3<byte[], byte[], Long>> {

    private final String topic;
    private final FlinkKafkaPartitioner<Long> partitioner;

    public LakeSoulDynamicKafkaRecordSerializationSchema(String topic, FlinkKafkaPartitioner<Long> partitioner) {
        this.topic = topic;
        this.partitioner = partitioner;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple3<byte[], byte[], Long> record, KafkaSinkContext kafkaSinkContext, Long aLong) {
        byte[] key = record.f0;
        byte[] value = record.f1;
        long hash = record.f2;
        Integer partition;
        if (this.partitioner != null) {
            partition = partitioner.partition(hash, key, value, topic,
                    kafkaSinkContext.getPartitionsForTopic(topic));
        } else {
            partition = null;
        }
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, partition, key, value);
        return producerRecord;
    }
}
