/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.dictionary;

import edu.uchicago.cs.db.common.functional.IntDoubleConsumer;
import edu.uchicago.cs.db.common.functional.IntIntConsumer;
import edu.uchicago.cs.db.common.functional.IntObjectConsumer;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;

/**
 * a simple implementation of dictionary for plain encoded values
 */
public abstract class PlainValuesDictionary extends Dictionary {

    /**
     * @param dictionaryPage the PLAIN encoded content of the dictionary
     * @throws IOException
     */
    protected PlainValuesDictionary(DictionaryPage dictionaryPage) throws IOException {
        super(dictionaryPage.getEncoding());
        if (dictionaryPage.getEncoding() != PLAIN_DICTIONARY
                && dictionaryPage.getEncoding() != PLAIN) {
            throw new ParquetDecodingException("Dictionary data encoding type not supported: " + dictionaryPage.getEncoding());
        }
    }

    /**
     * a simple implementation of dictionary for plain encoded binary
     */
    public static class PlainBinaryDictionary extends PlainValuesDictionary {

        private Binary[] binaryDictionaryContent = null;

        /**
         * Decodes {@link Binary} values from a {@link DictionaryPage}.
         * <p>
         * Values are read as length-prefixed values with a 4-byte little-endian
         * length.
         *
         * @param dictionaryPage a {@code DictionaryPage} of encoded binary values
         * @throws IOException
         */
        public PlainBinaryDictionary(DictionaryPage dictionaryPage) throws IOException {
            this(dictionaryPage, null);
        }

        /**
         * Decodes {@link Binary} values from a {@link DictionaryPage}.
         * <p>
         * If the given {@code length} is null, the values will be read as length-
         * prefixed values with a 4-byte little-endian length. If length is not
         * null, it will be used as the length for all fixed-length {@code Binary}
         * values read from the page.
         *
         * @param dictionaryPage a {@code DictionaryPage} of encoded binary values
         * @param length         a fixed length of binary arrays, or null if not fixed
         * @throws IOException
         */
        public PlainBinaryDictionary(DictionaryPage dictionaryPage, Integer length) throws IOException {
            super(dictionaryPage);
            final ByteBuffer dictionaryBytes = dictionaryPage.getBytes().toByteBuffer();
            binaryDictionaryContent = new Binary[dictionaryPage.getDictionarySize()];
            // dictionary values are stored in order: size (4 bytes LE) followed by {size} bytes
            int offset = dictionaryBytes.position();
            if (length == null) {
                // dictionary values are stored in order: size (4 bytes LE) followed by {size} bytes
                for (int i = 0; i < binaryDictionaryContent.length; i++) {
                    int len = readIntLittleEndian(dictionaryBytes, offset);
                    // read the length
                    offset += 4;
                    // wrap the content in a binary
                    binaryDictionaryContent[i] = Binary.fromConstantByteBuffer(dictionaryBytes, offset, len);
                    // increment to the next value
                    offset += len;
                }
            } else {
                // dictionary values are stored as fixed-length arrays
                Preconditions.checkArgument(length > 0,
                        "Invalid byte array length: " + length);
                for (int i = 0; i < binaryDictionaryContent.length; i++) {
                    // wrap the content in a Binary
                    binaryDictionaryContent[i] = Binary.fromConstantByteBuffer(
                            dictionaryBytes, offset, length);
                    // increment to the next value
                    offset += length;
                }
            }
        }

        @Override
        public int encodeBinary(Binary value) {
            int low = 0;
            int high = getMaxId();

            while (low <= high) {
                int mid = (low + high) >>> 1;
                Binary midVal = binaryDictionaryContent[mid];

                if (midVal.compareTo(value) < 0)
                    low = mid + 1;
                else if (midVal.compareTo(value) > 0)
                    high = mid - 1;
                else
                    return mid; // key found
            }
            return -(low + 1);  // key not found.
        }

        @Override
        public Binary decodeToBinary(int id) {
            return binaryDictionaryContent[id];
        }

        @Override
        public void access(IntObjectConsumer<Binary> consumer) {
            for (int i = 0; i < binaryDictionaryContent.length; i++) {
                consumer.consume(i, binaryDictionaryContent[i]);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PlainBinaryDictionary {\n");
            for (int i = 0; i < binaryDictionaryContent.length; i++) {
                sb.append(i).append(" => ").append(binaryDictionaryContent[i]).append("\n");
            }
            return sb.append("}").toString();
        }

        @Override
        public int getMaxId() {
            return binaryDictionaryContent.length - 1;
        }


    }

    /**
     * a simple implementation of dictionary for plain encoded long values
     */
    public static class PlainLongDictionary extends PlainValuesDictionary {

        private long[] longDictionaryContent = null;

        /**
         * @param dictionaryPage
         * @throws IOException
         */
        public PlainLongDictionary(DictionaryPage dictionaryPage) throws IOException {
            super(dictionaryPage);
            final ByteBuffer dictionaryByteBuf = dictionaryPage.getBytes().toByteBuffer();
            longDictionaryContent = new long[dictionaryPage.getDictionarySize()];
            LongPlainValuesReader longReader = new LongPlainValuesReader();
            longReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryByteBuf, dictionaryByteBuf.position());
            for (int i = 0; i < longDictionaryContent.length; i++) {
                longDictionaryContent[i] = longReader.readLong();
            }
        }

        @Override
        public long decodeToLong(int id) {
            return longDictionaryContent[id];
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PlainLongDictionary {\n");
            for (int i = 0; i < longDictionaryContent.length; i++) {
                sb.append(i).append(" => ").append(longDictionaryContent[i]).append("\n");
            }
            return sb.append("}").toString();
        }

        @Override
        public int getMaxId() {
            return longDictionaryContent.length - 1;
        }

    }

    /**
     * a simple implementation of dictionary for plain encoded double values
     */
    public static class PlainDoubleDictionary extends PlainValuesDictionary {

        private double[] doubleDictionaryContent = null;

        /**
         * @param dictionaryPage
         * @throws IOException
         */
        public PlainDoubleDictionary(DictionaryPage dictionaryPage) throws IOException {
            super(dictionaryPage);
            final ByteBuffer dictionaryByteBuf = dictionaryPage.getBytes().toByteBuffer();
            doubleDictionaryContent = new double[dictionaryPage.getDictionarySize()];
            DoublePlainValuesReader doubleReader = new DoublePlainValuesReader();
            doubleReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryByteBuf, 0);
            for (int i = 0; i < doubleDictionaryContent.length; i++) {
                doubleDictionaryContent[i] = doubleReader.readDouble();
            }
        }

        @Override
        public double decodeToDouble(int id) {
            return doubleDictionaryContent[id];
        }

        @Override
        public int encodeDouble(double value) {
            int low = 0;
            int high = getMaxId();

            while (low <= high) {
                int mid = (low + high) >>> 1;
                double midVal = doubleDictionaryContent[mid];

                if (midVal < value)
                    low = mid + 1;
                else if (midVal > value)
                    high = mid - 1;
                else
                    return mid; // key found
            }
            return -(low + 1);  // key not found.
        }

        @Override
        public void access(IntDoubleConsumer consumer) {
            for (int i = 0; i < doubleDictionaryContent.length; i++) {
                consumer.consume(i, doubleDictionaryContent[i]);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PlainDoubleDictionary {\n");
            for (int i = 0; i < doubleDictionaryContent.length; i++) {
                sb.append(i).append(" => ").append(doubleDictionaryContent[i]).append("\n");
            }
            return sb.append("}").toString();
        }

        @Override
        public int getMaxId() {
            return doubleDictionaryContent.length - 1;
        }

    }

    /**
     * a simple implementation of dictionary for plain encoded integer values
     */
    public static class PlainIntegerDictionary extends PlainValuesDictionary {

        private int[] intDictionaryContent = null;

        /**
         * @param dictionaryPage
         * @throws IOException
         */
        public PlainIntegerDictionary(DictionaryPage dictionaryPage) throws IOException {
            super(dictionaryPage);
            final ByteBuffer dictionaryByteBuf = dictionaryPage.getBytes().toByteBuffer();
            intDictionaryContent = new int[dictionaryPage.getDictionarySize()];
            IntegerPlainValuesReader intReader = new IntegerPlainValuesReader();
            intReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryByteBuf, 0);
            for (int i = 0; i < intDictionaryContent.length; i++) {
                intDictionaryContent[i] = intReader.readInteger();
            }
        }

        @Override
        public int decodeToInt(int id) {
            return intDictionaryContent[id];
        }

        @Override
        public int encodeInt(int value) {
            int low = 0;
            int high = getMaxId();

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int midVal = intDictionaryContent[mid];

                if (midVal < value)
                    low = mid + 1;
                else if (midVal > value)
                    high = mid - 1;
                else
                    return mid; // key found
            }
            return -(low + 1);  // key not found.
        }

        @Override
        public void access(IntIntConsumer consumer) {
            for (int i = 0; i < intDictionaryContent.length; i++) {
                consumer.consume(i, intDictionaryContent[i]);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PlainIntegerDictionary {\n");
            for (int i = 0; i < intDictionaryContent.length; i++) {
                sb.append(i).append(" => ").append(intDictionaryContent[i]).append("\n");
            }
            return sb.append("}").toString();
        }

        @Override
        public int getMaxId() {
            return intDictionaryContent.length - 1;
        }

    }

    /**
     * a simple implementation of dictionary for plain encoded float values
     */
    public static class PlainFloatDictionary extends PlainValuesDictionary {

        private float[] floatDictionaryContent = null;

        /**
         * @param dictionaryPage
         * @throws IOException
         */
        public PlainFloatDictionary(DictionaryPage dictionaryPage) throws IOException {
            super(dictionaryPage);
            final ByteBuffer dictionaryByteBuf = dictionaryPage.getBytes().toByteBuffer();
            floatDictionaryContent = new float[dictionaryPage.getDictionarySize()];
            FloatPlainValuesReader floatReader = new FloatPlainValuesReader();
            floatReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryByteBuf, dictionaryByteBuf.position());
            for (int i = 0; i < floatDictionaryContent.length; i++) {
                floatDictionaryContent[i] = floatReader.readFloat();
            }
        }

        @Override
        public float decodeToFloat(int id) {
            return floatDictionaryContent[id];
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PlainFloatDictionary {\n");
            for (int i = 0; i < floatDictionaryContent.length; i++) {
                sb.append(i).append(" => ").append(floatDictionaryContent[i]).append("\n");
            }
            return sb.append("}").toString();
        }

        @Override
        public int getMaxId() {
            return floatDictionaryContent.length - 1;
        }

    }

}
