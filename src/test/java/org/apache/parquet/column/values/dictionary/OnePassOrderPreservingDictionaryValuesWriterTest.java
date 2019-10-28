package org.apache.parquet.column.values.dictionary;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

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

    }
}