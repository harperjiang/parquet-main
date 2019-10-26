package org.apache.parquet.column.values.dictionary;

import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;

/**
 * <code>ExternalDictionaryValuesWriter</code> uses a pre-defined external dictionary to encode data from input.
 * If a given value is not in the dictionary, an <code>IllegalArgumentException</code> will be thrown
 * at runtime.
 * <p>
 * The generated data is compatible with <code>DictionaryValuesWriter</code> and can be accessed using
 * <code>DictionaryValuesReader</code>
 *
 * @author Hao Jiang
 */
public class ExternalDictionaryValuesWriter {

    public static class ExternalBinaryDictionaryValuesWriter extends DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter {

        /**
         * @param maxDictionaryByteSize
         * @param encodingForDataPage
         * @param encodingForDictionaryPage
         * @param allocator
         */
        public ExternalBinaryDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage,
                                                    Encoding encodingForDictionaryPage, ByteBufferAllocator allocator,
                                                    Object2IntMap<Binary> dictionary) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            this.binaryDictionaryContent = dictionary;
        }

        @Override
        public void writeBytes(Binary v) {
            if (!binaryDictionaryContent.containsKey(v)) {
                throw new IllegalArgumentException("dictionary does not contain the key:" + v.toStringUsingUTF8());
            }
            super.writeBytes(v);
        }
    }

    public static class ExternalIntegerDictionaryValuesWriter extends DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter {

        /**
         * @param maxDictionaryByteSize
         * @param encodingForDataPage
         * @param encodingForDictionaryPage
         * @param allocator
         */
        public ExternalIntegerDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage,
                                                     Encoding encodingForDictionaryPage, ByteBufferAllocator allocator,
                                                     Int2IntMap dictionary) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            this.intDictionaryContent = dictionary;
        }

        @Override
        public void writeInteger(int v) {
            if (!intDictionaryContent.containsKey(v)) {
                throw new IllegalArgumentException("dictionary does not contain the key:" + v);
            }
            super.writeInteger(v);
        }
    }

    public static class ExternalDoubleDictionaryValuesWriter extends DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter {

        /**
         * @param maxDictionaryByteSize
         * @param encodingForDataPage
         * @param encodingForDictionaryPage
         * @param allocator
         */
        public ExternalDoubleDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage,
                                                    Encoding encodingForDictionaryPage, ByteBufferAllocator allocator,
                                                    Double2IntMap dictionary) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            this.doubleDictionaryContent = dictionary;
        }

        @Override
        public void writeDouble(double v) {
            if (!doubleDictionaryContent.containsKey(v)) {
                throw new IllegalArgumentException("dictionary does not contain the key:" + v);
            }
            super.writeDouble(v);
        }
    }
}
