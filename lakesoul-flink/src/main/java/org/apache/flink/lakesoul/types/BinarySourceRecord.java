// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import io.debezium.data.Envelope;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.TimestampData;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BinarySourceRecord {

    private final String topic;

    private final List<String> primaryKeys;

    private final TableId tableId;

    private String tableLocation;

    private final List<String> partitionKeys;

    private final boolean isDDLRecord;

    private final LakeSoulRowDataWrapper data;

    private final String sourceRecordValue;

    public BinarySourceRecord(String topic, List<String> primaryKeys, TableId tableId, String tableLocation,
                              List<String> partitionKeys, boolean isDDLRecord, LakeSoulRowDataWrapper data,
                              String sourceRecordValue) {
        this.topic = topic;
        this.primaryKeys = primaryKeys;
        this.tableId = tableId;
        this.tableLocation = tableLocation;
        this.partitionKeys = partitionKeys;
        this.isDDLRecord = isDDLRecord;
        this.data = data;
        this.sourceRecordValue = sourceRecordValue;
    }

    public BinarySourceRecord(String topic, List<String> primaryKeys,
                              List<String> partitionKeys,
                              LakeSoulRowDataWrapper data,
                              String sourceRecordValue,
                              TableId tableId,
                              boolean isDDL) {
        this.topic = topic;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.data = data;
        this.sourceRecordValue = sourceRecordValue;
        this.tableId = tableId;
        this.isDDLRecord = isDDL;
    }

    public static BinarySourceRecord fromMysqlSourceRecord(SourceRecord sourceRecord,
                                                           LakeSoulRecordConvert convert,
                                                           String basePath) throws Exception {
        Schema keySchema = sourceRecord.keySchema();
        TableId tableId = new TableId(io.debezium.relational.TableId.parse(sourceRecord.topic()).toLowercase());
        boolean isDDL = "io.debezium.connector.mysql.SchemaChangeKey".equalsIgnoreCase(keySchema.name());
        if (isDDL) {
            return null;
        } else {
            List<String> primaryKeys = new ArrayList<>();
            keySchema.fields().forEach(f -> primaryKeys.add(f.name()));
            Schema valueSchema = sourceRecord.valueSchema();
            Struct value = (Struct) sourceRecord.value();

            // retrieve source event time if exist and non-zero
            Field sourceField = valueSchema.field(Envelope.FieldName.SOURCE);
            long binlogFileIndex = 0;
            long binlogPosition = 0;
            long tsMs = 0;
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            if (sourceField != null && source != null) {
                if (sourceField.schema().field("file") != null) {
                    String fileName = (String) source.getWithoutDefault("file");
                    if (StringUtils.isNotBlank(fileName)) {
                        binlogFileIndex = Long.parseLong(fileName.substring(fileName.lastIndexOf(".") + 1));
                    }
                }
                if (sourceField.schema().field("pos") != null) {
                    binlogPosition = (Long) source.getWithoutDefault("pos");
                }
                if (sourceField.schema().field("ts_ms") != null) {
                    tsMs = (Long) source.getWithoutDefault("ts_ms");
                }
            }
            long sortField = (binlogFileIndex << 32) + binlogPosition;
            LakeSoulRowDataWrapper data = convert.toLakeSoulDataType(valueSchema, value, tableId, tsMs, sortField);
            String tablePath;
            if (tableId.schema() == null){
                tablePath = new Path(new Path(basePath, tableId.catalog()), tableId.table()).toString();
            }else {
                tablePath = new Path(new Path(basePath, tableId.schema()), tableId.table()).toString();
            }
            return new BinarySourceRecord(sourceRecord.topic(), primaryKeys, tableId, FlinkUtil.makeQualifiedPath(tablePath).toString(),
                    Collections.emptyList(), false, data, null);
        }
    }

    public static BinarySourceRecord fromKafkaSourceRecord(ConsumerRecord consumerRecord,
                                                           LakeSoulRecordConvert convert,
                                                           String basePath,
                                                           String dbName,
                                                           ObjectMapper objectMapper) throws Exception {

        String topic = consumerRecord.topic();
        int partition = consumerRecord.partition();
        long offset = consumerRecord.offset();

        JsonNode keyNode = objectMapper.readTree((byte[]) consumerRecord.key());
        JsonNode valueNode = objectMapper.readTree((byte[]) consumerRecord.value());

        List<String> keyList = new ArrayList<>();
//        JsonNode primaryKeys = keyNode.get("primary_keys");
//        if (primaryKeys.isArray()) {
//            Iterator<JsonNode> elements = primaryKeys.elements();
//            while (elements.hasNext()){
//                keyList.add(elements.next().asText());
//            }
//        }
//        JsonNode pkValue = keyNode.get("pk_value");
        Iterator<String> pkIterator = keyNode.fieldNames();
        while (pkIterator.hasNext()) {
            String keyName = pkIterator.next();
            keyList.add(keyName);
        }

        JsonNode source = valueNode.get("source");
        String databaseName = source.get("db").asText();
        String tableName = source.get("table").asText();

        if (StringUtils.isEmpty(dbName)) {
            dbName = databaseName;
        }
        String realTable = String.format("s_%s_%s", databaseName, tableName);
        TableId tableId = new TableId("lakesoul", dbName, realTable);

        String opType = valueNode.get("op").asText();
        long tsMs = source.get("ts_ms").asLong();
        String beforeTypeStr = null;
        JsonNode before = valueNode.get("before");
        if (!before.isEmpty() && !before.isNull()) {
            beforeTypeStr = before.get("field_type").asText();
            ((ObjectNode) before).remove("field_type");
        }
        String afterTypeStr = null;
        JsonNode after = valueNode.get("after");
        if (!after.isEmpty() && !after.isNull()) {
            afterTypeStr = after.get("field_type").asText();
            ((ObjectNode) after).remove("field_type");
        }

        long sortField = offset;

        LakeSoulRowDataWrapper data = convert.kafkaToLakeSoulDataType(before, beforeTypeStr, after, afterTypeStr, opType, tableId, keyList, tsMs, sortField);
        String tablePath = new Path(new Path(basePath, tableId.schema()), tableId.table()).toString();

        return new BinarySourceRecord(tableId.toString(), keyList, tableId, FlinkUtil.makeQualifiedPath(tablePath).toString(),
                Collections.emptyList(), false, data, null);
    }

    public static BinarySourceRecord fromKafkaAvroSourceRecord(ConsumerRecord consumerRecord,
                                                           GenericRecord keyRecord,
                                                           GenericRecord valueRecord,
                                                           LakeSoulRecordConvert convert,
                                                           String basePath,
                                                           String dbName,
                                                           ObjectMapper objectMapper) throws Exception {


        long offset = consumerRecord.offset();
        org.apache.avro.Schema valueSchema = valueRecord.getSchema();

        JsonNode keyNode = objectMapper.readTree(keyRecord.toString());
        JsonNode valueSchemaNode = objectMapper.readTree(valueSchema.toString()).get("fields");
        JsonNode valueNode = objectMapper.readTree(valueRecord.toString());

        List<String> keyList = new ArrayList<>();
        Iterator<String> pkIterator = keyNode.fieldNames();
        while (pkIterator.hasNext()) {
            String keyName = pkIterator.next();
            keyList.add(keyName);
        }

        String opType = valueNode.get("op_type").asText();
        ((ObjectNode) valueNode).remove("op_type");
        long tsMs = valueNode.get("op_ts").asLong();
        ((ObjectNode) valueNode).remove("op_ts");

        String databaseAndTable = valueNode.get("table").asText();
        String databaseName = "";
        String tableName = "";
        String[] split = databaseAndTable.split("\\.");
        if (split.length != 2) {
            throw new IllegalArgumentException(
                    String.format("This record database and table info ERROR, attribute 'table' should be the format of 'database.table'," +
                            "current is 'table': %s", databaseAndTable));
        } else {
            databaseName = split[0];
            tableName = split[1];
            if (StringUtils.isBlank(databaseName) || StringUtils.isBlank(tableName)) {
                throw new IllegalArgumentException(String.format("This record database and table info be lost, 'table': %s", databaseAndTable));
            }
            ((ObjectNode) valueNode).remove("table");
        }
        if (StringUtils.isEmpty(dbName)) {
            dbName = databaseName;
        }
        String realTable = String.format("s_%s_%s", databaseName, tableName);
        TableId tableId = new TableId("lakesoul", dbName, realTable);

        long sortField = offset;

        LakeSoulRowDataWrapper data = convert.kafkaAvroToLakeSoulDataType(valueNode, valueSchemaNode, opType, tableId, keyList, tsMs, sortField);
        String tablePath = new Path(new Path(basePath, tableId.schema()), tableId.table()).toString();

        return new BinarySourceRecord(tableId.toString(), keyList, tableId, FlinkUtil.makeQualifiedPath(tablePath).toString(),
                Collections.emptyList(), false, data, null);
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public boolean isDDLRecord() {
        return isDDLRecord;
    }

    public LakeSoulRowDataWrapper getData() {
        return data;
    }

    public SchemaAndValue getDDLStructValue() {
        SourceRecordJsonSerde serde = SourceRecordJsonSerde.getInstance();
        return serde.deserializeValue(topic, sourceRecordValue);
    }

    @Override
    public String toString() {
        return "BinarySourceRecord{" +
                "topic='" + topic + '\'' +
                ", primaryKeys=" + primaryKeys +
                ", tableId=" + tableId +
                ", tableLocation='" + tableLocation + '\'' +
                ", partitionKeys=" + partitionKeys +
                ", isDDLRecord=" + isDDLRecord +
                ", data=" + data +
                ", sourceRecordValue='" + sourceRecordValue + '\'' +
                '}';
    }
}
