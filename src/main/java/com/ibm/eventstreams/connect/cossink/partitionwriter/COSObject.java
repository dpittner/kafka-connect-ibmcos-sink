package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cos.Bucket;
import org.apache.kafka.connect.sink.SinkRecord;

public interface COSObject {
    void put(SinkRecord record);

    void write(Bucket bucket);

    Long lastOffset();

}
