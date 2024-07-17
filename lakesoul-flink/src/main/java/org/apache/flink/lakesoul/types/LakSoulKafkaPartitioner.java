// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

public class LakSoulKafkaPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final long serialVersionUID = -3785320239953858771L;

    private int parallelInstanceId;

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(
                parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                parallelInstances > 0, "Number of subtasks must be larger than 0.");

        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");

        if (record != null) {
            if (record instanceof Long) {
                return partitions[(int) ((Long) record % partitions.length)];
            }
        }
        return partitions[parallelInstanceId % partitions.length];
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof FlinkFixedPartitioner;
    }

    @Override
    public int hashCode() {
        return FlinkFixedPartitioner.class.hashCode();
    }
}