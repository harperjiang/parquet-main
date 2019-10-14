package edu.uchicago.cs.db.common;

import org.apache.parquet.column.impl.ForwardIterator;
import org.apache.parquet.column.impl.FullForwardIterator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FullForwardIteratorTest {

    @Test
    void startfrom() {

        ForwardIterator fi = new FullForwardIterator(4000);
        for (int i = 0; i < 100; i++) {
            assertEquals(i, fi.nextLong());
        }

        fi.startfrom(1010);
        assertEquals(1010,fi.nextLong());
        fi.startfrom(1200);
        assertEquals(1200,fi.nextLong());
    }

    @Test
    void limit() {
        ForwardIterator fi = new FullForwardIterator(100);
        int counter = 0;
        while(fi.hasNext()) {
            fi.nextLong();
            counter++;
        }
        assertEquals(100,counter);
    }
}