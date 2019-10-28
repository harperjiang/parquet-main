package org.apache.parquet.column.values.dictionary;

import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class OnePassOrderPreservingDictionaryTest {

    @Test
    public void binary() {

        String[] values = new String[50];
        int counter = 0;
        PlainValuesWriter writer = new PlainValuesWriter(ParquetProperties.DEFAULT_PAGE_SIZE,
                ParquetProperties.DEFAULT_PAGE_SIZE, new HeapByteBufferAllocator());
        writer.writeInteger(4);
        writer.writeInteger(50);

        // First section has 40 entries
        writer.writeInteger(40);
        for (int i = 0; i < 40; i++) {
            String uuid = UUID.randomUUID().toString();
            values[counter++] = uuid;
            writer.writeBytes(Binary.fromString(uuid));
        }
        // Second section has no entries
        writer.writeInteger(0);

        // Third section has 8 entries
        writer.writeInteger(8);
        for (int i = 0; i < 8; i++) {
            String uuid = UUID.randomUUID().toString();
            values[counter++] = uuid;
            writer.writeBytes(Binary.fromString(uuid));
        }
        // Last section has 2 entries
        writer.writeInteger(2);
        for (int i = 0; i < 2; i++) {
            String uuid = UUID.randomUUID().toString();
            values[counter++] = uuid;
            writer.writeBytes(Binary.fromString(uuid));
        }

        DictionaryPage dictPage = new DictionaryPage(writer.getBytes(), (int) writer.getBufferedSize(), Encoding.OPOP_DICTIONARY);

        OnePassOrderPreservingDictionary.BinaryDictionary dict = new OnePassOrderPreservingDictionary.BinaryDictionary(dictPage);

        assertEquals(49, dict.getMaxId());

        Arrays.sort(values, 0, 40);
        dict.nextPage();
        for (int i = 0; i < 40; i++) {
            assertEquals(values[i], dict.decodeToBinary(i).toStringUsingUTF8());
        }
        for (int i = 0; i < 40; i++) {
            assertEquals(i, dict.encodeBinary(Binary.fromString(values[i])));
        }

        dict.nextPage();
        for (int i = 0; i < 40; i++) {
            assertEquals(values[i], dict.decodeToBinary(i).toStringUsingUTF8());
        }
        for (int i = 0; i < 40; i++) {
            assertEquals(i, dict.encodeBinary(Binary.fromString(values[i])));
        }

        dict.nextPage();
        Arrays.sort(values, 0, 48);
        for (int i = 0; i < 48; i++) {
            assertEquals(values[i], dict.decodeToBinary(i).toStringUsingUTF8());
        }
        for (int i = 0; i < 48; i++) {
            assertEquals(i, dict.encodeBinary(Binary.fromString(values[i])));
        }

        dict.nextPage();
        Arrays.sort(values, 0, 50);
        for (int i = 0; i < 50; i++) {
            assertEquals(values[i], dict.decodeToBinary(i).toStringUsingUTF8());
        }
        for (int i = 0; i < 50; i++) {
            assertEquals(i, dict.encodeBinary(Binary.fromString(values[i])));
        }
    }
}