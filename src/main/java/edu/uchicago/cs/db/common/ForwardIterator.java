package edu.uchicago.cs.db.common;

import it.unimi.dsi.fastutil.longs.LongIterator;

public interface ForwardIterator extends LongIterator {

    void forwardto(long pos);

}
