package org.apache.parquet.column.values.vbp;

import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.vbp.VarlenBitPackingValuesReader;
import org.apache.parquet.column.values.vbp.VarlenBitPackingValuesWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class VarlenBitPackingValuesReaderTest {

    @Test
    void readInteger() throws Exception {
        VarlenBitPackingValuesWriter writer = new VarlenBitPackingValuesWriter(
                ParquetProperties.builder().build().getInitialSlabSize(),
                ParquetWriter.DEFAULT_PAGE_SIZE, new DirectByteBufferAllocator());
        VarlenBitPackingValuesReader reader = new VarlenBitPackingValuesReader();

        Random rand = new Random();
        List<Integer> recorder = new ArrayList();
        int count = 1000000;
        for (int i = 0; i < count; i++) {
            int value = rand.nextInt() & 0x7FFFFFFF;
            writer.writeInteger(value);
            recorder.add(value);
        }

        ByteBuffer buffer = writer.getBytes().toByteBuffer();

        reader.initFromPage(count, buffer, 0);

        for (int i = 0; i < count; i++) {
            assertEquals(recorder.get(i), reader.readInteger(), String.valueOf(i));
        }
    }

    @Test
    void skip() throws Exception {
        VarlenBitPackingValuesWriter writer = new VarlenBitPackingValuesWriter(
                ParquetProperties.builder().build().getInitialSlabSize(),
                ParquetWriter.DEFAULT_PAGE_SIZE, new DirectByteBufferAllocator());
        VarlenBitPackingValuesReader reader = new VarlenBitPackingValuesReader();

        Random rand = new Random();
        List<Integer> recorder = new ArrayList();
        int count = 1000000;
        for (int i = 0; i < count; i++) {
            int value = rand.nextInt() & 0x7FFFFFFF;
            writer.writeInteger(value);
            recorder.add(value);
        }

        ByteBuffer buffer = writer.getBytes().toByteBuffer();

        reader.initFromPage(count, buffer, 0);

        reader.skip(10);
        assertEquals(recorder.get(10), reader.readInteger());

        reader.skip(100);
        assertEquals(recorder.get(111), reader.readInteger());

        reader.skip(1000);
        assertEquals(recorder.get(1112), reader.readInteger());
    }
}