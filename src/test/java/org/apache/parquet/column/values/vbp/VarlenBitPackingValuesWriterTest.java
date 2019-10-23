package org.apache.parquet.column.values.vbp;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.vbp.VarlenBitPackingValuesWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class VarlenBitPackingValuesWriterTest {

    @Test
    void writeSimpleInteger() throws Exception {
        VarlenBitPackingValuesWriter writer = new VarlenBitPackingValuesWriter(
                ParquetProperties.builder().build().getInitialSlabSize(),
                ParquetWriter.DEFAULT_PAGE_SIZE, new DirectByteBufferAllocator());

        assertEquals(Encoding.VARLEN_BIT_PACKED, writer.getEncoding());

        for (int i = 0; i < 10000; i++) {
            writer.writeInteger(i);
        }

        BytesInput input = writer.getBytes();
        byte[] bytes = input.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        // the length
        BytesUtils.readIntLittleEndian(in);


        byte[] buffer = new byte[36];
        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(9);
        int[] result = new int[32];

        for (int rep = 0; rep < 20; rep++) {
            // the first group contains 0-511, bit width should be 9
            assertEquals(9, in.read(), String.valueOf(rep));
            assertEquals(512 * rep, BytesUtils.readZigZagVarInt(in));
            for (int i = 0; i < 16; i++) {
                in.read(buffer);
                packer.unpack32Values(buffer, 0, result, 0);
                for (int j = 0; j < 32; j++) {
                    assertEquals(i * 32 + j, result[j]);
                }
            }
        }
    }

    @Test
    void writeLargeInteger() throws Exception {
        VarlenBitPackingValuesWriter writer = new VarlenBitPackingValuesWriter(
                ParquetProperties.builder().build().getInitialSlabSize(),
                ParquetWriter.DEFAULT_PAGE_SIZE, new DirectByteBufferAllocator());

        assertEquals(Encoding.VARLEN_BIT_PACKED, writer.getEncoding());

        Random rand = new Random(339);
        List<Integer> recorder = new ArrayList<>();
        try {
            for (int i = 0; i < 10000; i++) {
                int value = rand.nextInt();
                writer.writeInteger(value);
                recorder.add(value);
            }
            fail("Number will overflow");
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    void writeNormalInteger() throws Exception {
        VarlenBitPackingValuesWriter writer = new VarlenBitPackingValuesWriter(
                ParquetProperties.builder().build().getInitialSlabSize(),
                ParquetWriter.DEFAULT_PAGE_SIZE, new DirectByteBufferAllocator());

        assertEquals(Encoding.VARLEN_BIT_PACKED, writer.getEncoding());

        Random rand = new Random(339);
        List<Integer> recorder = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            int value = rand.nextInt() & 0x7FFFFFFF;
            writer.writeInteger(value);
            recorder.add(value);
        }

        ByteArrayInputStream input = new ByteArrayInputStream(writer.getBytes().toByteArray());

        // length
        BytesUtils.readIntLittleEndian(input);
        // bit width
        for (int rep = 0; rep < 19; rep++) {
            int bitWidth = input.read();
            int base = BytesUtils.readZigZagVarInt(input);
            BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
            byte[] buffer = new byte[4 * bitWidth];
            int[] result = new int[32];

            for (int i = 0; i < 16; i++) {
                input.read(buffer);
                packer.unpack32Values(buffer, 0, result, 0);

                for (int j = 0; j < 32; j++) {
                    assertEquals(recorder.get(rep * 512 + 32 * i + j), result[j] + base);
                }
            }
        }

        int bitWidth = input.read();
        int base = BytesUtils.readZigZagVarInt(input);
        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        byte[] buffer = new byte[4 * bitWidth];
        int[] result = new int[32];

        for (int i = 0; i < 16; i++) {
            input.read(buffer);
            packer.unpack32Values(buffer, 0, result, 0);

            for (int j = 0; j < 32; j++) {
                int index = 19 * 512 + 32 * i + j;
                if (index < 10000) {
                    assertEquals(recorder.get(index), result[j] + base);
                }
            }
        }

    }
}