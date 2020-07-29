package com.ibm.eventstreams.connect.cossink.partitionwriter;

public class COSObjectFactory {

    private boolean delimiter;
    private String avroSchema;

    public COSObjectFactory(final boolean delimiter, final String avroSchema) {
        this.delimiter = delimiter;
        this.avroSchema = avroSchema;
    }

    public COSObject newCOSObject() {
        if (avroSchema == null) {
            return new COSObjectImpl(delimiter);
        }
        return new COSObjectParquet(avroSchema);
    };
}
