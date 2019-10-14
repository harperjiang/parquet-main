package org.apache.parquet.column.impl;

public class FullForwardIterator implements ForwardIterator {

    long counter = 0;

    long limit = 0;

    public FullForwardIterator(long limit) {
        this.limit = limit;
    }

    @Override
    public void startfrom(long pos) {
        counter = pos;
    }

    @Override
    public long nextLong() {
        return counter++;
    }

    @Override
    public boolean hasNext() {
        return counter < limit;
    }
}
