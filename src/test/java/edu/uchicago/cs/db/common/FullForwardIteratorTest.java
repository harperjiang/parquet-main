package edu.uchicago.cs.db.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FullForwardIteratorTest {

    @Test
    void startfrom() {

        ForwardIterator fi = new FullForwardIterator();
        for (int i = 0; i < 100; i++) {
            assertEquals(i, fi.nextLong());
        }

        fi.startfrom(1010);
        assertEquals(1010,fi.nextLong());
        fi.startfrom(1200);
        assertEquals(1200,fi.nextLong());
    }
}