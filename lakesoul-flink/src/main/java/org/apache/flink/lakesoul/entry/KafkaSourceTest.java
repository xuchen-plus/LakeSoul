package org.apache.flink.lakesoul.entry;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.BinaryKafkaRecordDeserializationSchema;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.WAREHOUSE_PATH;


public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env;

//        env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(2000);

        conf.set(LakeSoulSinkOptions.WAREHOUSE_PATH, "/Users/dudongfeng/work/zehy/lakesoul/11");
        conf.set(LakeSoulSinkOptions.USE_CDC, true);

        LakeSoulRecordConvert lakeSoulRecordConvert = new LakeSoulRecordConvert(conf, "UTC");

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "my-group-test-2");
//        properties.setProperty("auto.offset.reset.strategy", "latest");


        KafkaSource<BinarySourceRecord> source = KafkaSource.<BinarySourceRecord>builder()
//                .setProperties(properties)
                .setBootstrapServers("localhost:9092")
                .setTopics("test")
                .setGroupId("my-group-test-2")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new BinaryKafkaRecordDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH), null))
                .build();



//        DataStreamSource<BinarySourceRecord> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        kafkaSource.print();

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = (Configuration) env.getConfiguration();
        LakeSoulMultiTableSinkStreamBuilder builder = new LakeSoulMultiTableSinkStreamBuilder(source, context, lakeSoulRecordConvert);
        DataStreamSource<BinarySourceRecord> sourceA = builder.buildMultiTableSource("Kafka Source");
//
        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(sourceA);
        DataStreamSink<BinarySourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);

        env.execute("testAA");
    }
}
