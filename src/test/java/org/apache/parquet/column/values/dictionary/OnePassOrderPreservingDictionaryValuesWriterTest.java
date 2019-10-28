package org.apache.parquet.column.values.dictionary;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OnePassOrderPreservingDictionaryValuesWriterTest {

    @Test
    public void binary() throws IOException {
        ValuesWriter writer = new OnePassOrderPreservingDictionaryValuesWriter.BinaryWriter(ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE,
                new HeapByteBufferAllocator());
        String[] values = new String[1000];
        BytesInput[] pages = new BytesInput[5];
        int pageCounter = 0;
        for (int i = 1; i <= 1000; i++) {
            String uuid = UUID.randomUUID().toString();
            values[i - 1] = uuid;
            writer.writeBytes(Binary.fromString(uuid));
            if (i % 200 == 0) {
                // Create pages
                pages[pageCounter++] = writer.getBytes();
            }
        }

        DictionaryPage dictPage = writer.toDictPageAndClose();

        Dictionary dict = dictPage.getEncoding().initDictionary(new ColumnDescriptor(new String[]{"name"},
                PrimitiveType.PrimitiveTypeName.BINARY, 0, 0), dictPage);
        assertTrue(dict instanceof OnePassOrderPreservingDictionary.BinaryDictionary);
        OnePassOrderPreservingDictionary.BinaryDictionary bdict = (OnePassOrderPreservingDictionary.BinaryDictionary) dict;

        assertEquals(0, bdict.entrySize);

        for (int pi = 0; pi < 5; pi++) {
            bdict.nextPage();
            assertEquals(200 * (pi + 1), bdict.entrySize);
            InputStream in = pages[pi].toInputStream();
            int bitWidth = BytesUtils.readIntLittleEndianOnOneByte(in);
            RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);
            for (int i = 0; i < 200; i++) {
                assertEquals(values[i], bdict.decodeToBinary(decoder.readInt()).toStringUsingUTF8(), String.valueOf(i));
            }
        }
    }
}