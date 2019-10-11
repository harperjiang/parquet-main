package edu.uchicago.cs.db.common;

public class FullForwardIterator implements ForwardIterator {

    long counter = 0;

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
        return true;
    }
}
