package org.apache.parquet.column.impl;

import it.unimi.dsi.fastutil.longs.LongIterator;

public interface ForwardIterator extends LongIterator {

    void startfrom(long pos);

}
