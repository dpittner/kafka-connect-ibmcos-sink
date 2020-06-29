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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

class COSObjectParquet {
    private static final Logger LOG = LoggerFactory.getLogger(COSObjectParquet.class);

    private static final byte[] EMPTY = new byte[0];

    private final List<SinkRecord> records = new LinkedList<>();
    private Long lastOffset;


    COSObjectParquet() {
    }

    void put(SinkRecord record) {
        LOG.trace("> put, {}-{} offset={}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
        records.add(record);
        lastOffset = record.kafkaOffset();
        LOG.trace("< put");
    }

    void write(final Bucket bucket) {
        LOG.trace("> write, records.size={} lastOffset={}", records.size(), lastOffset);
        if (records.isEmpty()) {
            throw new IllegalStateException("Attempting to write an empty object");
        }

        final String key = createKey();
        final InputStream is = createStream();

        bucket.putObject(key, is, createMetadata(key, value));
        LOG.trace("< write, key={}", key);
    }

    Long lastOffset() {
        return lastOffset;
    }

    String createKey() {
        SinkRecord firstRecord = records.get(0);
        return String.format("%s/%d/%016d-%016d",
                firstRecord.topic(), firstRecord.kafkaPartition(), firstRecord.kafkaOffset(), lastOffset);
    }

    InputStream createStream() {
        Path filePath = Paths.get("./example.parquet");
        int blockSize = 1024;
        int pageSize = 65535;
        try(
                AvroParquetWriter parquetWriter = new AvroParquetWriter(
                        filePath,
                        avroSchema,
                        CompressionCodecName.SNAPPY,
                        blockSize,
                        pageSize)
        ){
            for(SinkRecord obj : records){
                parquetWriter.write(obj);
            }
            return Files.newInputStream(filePath);
        }catch(java.io.IOException e){
            System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
            e.printStackTrace();
        }


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

    private static ObjectMetadata createMetadata(final String key, final byte[] value) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(value.length);
        return metadata;
    }
}
