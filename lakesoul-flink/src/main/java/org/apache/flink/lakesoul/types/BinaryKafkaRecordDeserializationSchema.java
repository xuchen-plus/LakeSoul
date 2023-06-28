package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.lakesoul.tool.JacksonMapperFactory;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class BinaryKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<BinarySourceRecord> {
    LakeSoulRecordConvert convert;
    String basePath;
    private ObjectMapper objectMapper;

    public BinaryKafkaRecordDeserializationSchema(LakeSoulRecordConvert convert, String basePath) {
        this.convert = convert;
        this.basePath = basePath;
        objectMapper = new ObjectMapper();
//        objectMapper = JacksonMapperFactory.createObjectMapper()
//                        .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(),true);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<BinarySourceRecord> collector) throws IOException {
        try {
            collector.collect(BinarySourceRecord.fromKafkaSourceRecord(consumerRecord, this.convert, this.basePath, objectMapper));
        } catch (Exception e) {
            throw new IOException(String.format("Failed to deserialize consumer record %s.", e.getMessage()), e);
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(new TypeHint<BinarySourceRecord>() {});
    }

}
