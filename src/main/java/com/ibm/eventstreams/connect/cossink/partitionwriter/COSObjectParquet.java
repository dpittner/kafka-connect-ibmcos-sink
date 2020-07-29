/*
 * Copyright 2019 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

class COSObjectParquet implements COSObject {
    private static final Logger LOG = LoggerFactory.getLogger(COSObjectParquet.class);

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final byte[] EMPTY = new byte[0];

    private final List<SinkRecord> records = new LinkedList<>();
    private Long lastOffset;

    private final JsonAvroConverter converter = new JsonAvroConverter();
    private Path tempFile;
    private org.apache.avro.Schema avroSchema;

    COSObjectParquet(String schema){
        try {
            tempFile = Files.createTempFile("cos-sink-", ".parqet");
            avroSchema = new org.apache.avro.Schema.Parser().parse(schema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void put(SinkRecord record) {
        LOG.trace("> put, {}-{} offset={}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
        records.add(record);
        lastOffset = record.kafkaOffset();
        LOG.trace("< put");
    }

    @Override
    public void write(final Bucket bucket) {
        LOG.trace("> write, records.size={} lastOffset={}", records.size(), lastOffset);
        if (records.isEmpty()) {
            throw new IllegalStateException("Attempting to write an empty object");
        }
        try {
            final String key = createKey();
            final InputStream is = createStream();

            bucket.putObject(key, is, createMetadata());
            LOG.trace("< write, key={}", key);
        } catch (IOException e) {
            LOG.warn("< failed to write parquet, error={}", e);
            throw new RuntimeException(e);
        } finally {
            try {
                Files.delete(tempFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public Long lastOffset() {
        return lastOffset;
    }


    String createKey() {
        SinkRecord firstRecord = records.get(0);
        return String.format("topic=%s/partition=%d/offsetBegin=%016d/offsetEnd=%016d/kafka.snappy.parquet",
                firstRecord.topic(), firstRecord.kafkaPartition(), firstRecord.kafkaOffset(), lastOffset);
    }

    InputStream createStream() throws IOException {

        OutputFile oFile = TempOutputfile.nioPathToOutputFile(tempFile);
        ParquetWriter<GenericData.Record> parquetWriter = AvroParquetWriter
                .<GenericData.Record>builder(oFile)
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

        for(SinkRecord obj : records){
            GenericData.Record aRecord = converter.convertToGenericDataRecord(createValue(obj), avroSchema);
            parquetWriter.write(aRecord);
        }
        parquetWriter.close();
        return Files.newInputStream(tempFile);
    }

    private static byte[] createValue(final SinkRecord record) {
        final Schema schema = record.valueSchema();
        byte[] result = null;
        if (schema == null || schema.type() == Type.BYTES) {
            if (record.value() instanceof byte[]) {
                result = (byte[])record.value();
            } else if (record.value() instanceof ByteBuffer) {
                final ByteBuffer bb = (ByteBuffer)record.value();
                result = new byte[bb.remaining()];
                bb.get(result);
            }
        }

        if (result == null) {
            Object value = record.value();
            if (value != null) {
                result = value.toString().getBytes(UTF8);
            } else {
                result = EMPTY;
            }
        }
        return result;
    }

    private ObjectMetadata createMetadata() throws IOException {
        ObjectMetadata metadata = new ObjectMetadata();
        FileChannel tempFileChannel = FileChannel.open(tempFile);

        metadata.setContentLength(tempFileChannel.size());
        tempFileChannel.close();
        return metadata;
    }
}
