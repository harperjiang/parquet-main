package org.apache.parquet.column.values.dictionary;


import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

/**
 * OnePassOrderPreservingDictionaryValuesWriter generate order-preserving dictionary for each
 * page
 */

public class OnePassOrderPreservingDictionaryValuesWriter {

    public class BinaryDictionaryValuesWriter extends DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter {

        boolean firstPage = true;

        /**
         * @param maxDictionaryByteSize
         * @param encodingForDataPage
         * @param encodingForDictionaryPage
         * @param allocator
         */
        public BinaryDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage,
                                            Encoding encodingForDictionaryPage, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
        }

        @Override
        public BytesInput getBytes() {
            // This marks the end of a page
            // Sort the dictionary and re-encode the key
            sortDictionaryThenRecode();
            if (firstPage) {
                firstPage = false;
            }
            return super.getBytes();
        }

        @Override
        public void writeBytes(Binary v) {
            if (!firstPage && !binaryDictionaryContent.containsKey(v)) {
                // Record the newly inserted keys in this round

            }
            super.writeBytes(v);
        }

        @Override
        public DictionaryPage toDictPageAndClose() {

            return super.toDictPageAndClose();
        }

        protected void sortDictionaryThenRecode() {

        }
    }
}
