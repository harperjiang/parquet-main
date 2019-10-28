package org.apache.parquet.column.values.dictionary;


import it.unimi.dsi.fastutil.doubles.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;

import java.util.*;

/**
 * OnePassOrderPreservingDictionaryValuesWriter generate order-preserving dictionary for each
 * page
 */

public class OnePassOrderPreservingDictionaryValuesWriter {

    public static class BinaryWriter extends DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter {

        List<Set<Binary>> dictionarySections;

        /**
         * @param maxDictionaryByteSize
         * @param allocator
         */
        public BinaryWriter(int maxDictionaryByteSize, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, Encoding.RLE_DICTIONARY, Encoding.OPOP_DICTIONARY, allocator);
            dictionarySections = new ArrayList<>();
            dictionarySections.add(new HashSet<>());
        }

        @Override
        public BytesInput getBytes() {
            // This marks the end of a page
            // Sort the dictionary and re-encode the key
            if (!dictionarySections.get(dictionarySections.size() - 1).isEmpty()) {
                sortDictionaryThenRecode();
            }
            dictionarySections.add(new HashSet<>());
            return super.getBytes();
        }

        @Override
        public void writeBytes(Binary v) {
            if (!binaryDictionaryContent.containsKey(v)) {
                // Record the newly inserted keys in this section
                dictionarySections.get(dictionarySections.size() - 1).add(v);
            }
            super.writeBytes(v);
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                if (dictionarySections.get(dictionarySections.size() - 1).isEmpty()) {
                    dictionarySections.remove(dictionarySections.size() - 1);
                }

                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);

                // Write the number of sections
                dictionaryEncoder.writeInteger(dictionarySections.size());
                // Write the number of total elements for easy space allocation when reading
                dictionaryEncoder.writeInteger(binaryDictionaryContent.size());
                // Write the length of each section, and the content
                dictionarySections.forEach(sec -> {
                    dictionaryEncoder.writeInteger(sec.size());
                    sec.forEach(dictionaryEncoder::writeBytes);
                });
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        protected void sortDictionaryThenRecode() {
            // Sort the entry in dictionary content and re-encode the entries
            Object[] sorted = binaryDictionaryContent.keySet().toArray();
            Arrays.sort(sorted);
            Int2IntMap mapping = new Int2IntOpenHashMap();
            Object2IntMap<Binary> newBinaryDictionaryContent = new Object2IntLinkedOpenHashMap<>();
            newBinaryDictionaryContent.defaultReturnValue(-1);
            for (int i = 0; i < sorted.length; i++) {
                mapping.put(binaryDictionaryContent.getInt(sorted[i]), i);
                newBinaryDictionaryContent.put((Binary) sorted[i], i);
            }
            binaryDictionaryContent = newBinaryDictionaryContent;

            IntList newEncodedValues = new IntList();
            IntList.IntIterator it = encodedValues.iterator();
            while (it.hasNext()) {
                newEncodedValues.add(mapping.get(it.next()));
            }
            encodedValues = newEncodedValues;
        }
    }

    public static class IntegerWriter extends DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter {

        List<IntSet> dictionarySections;

        /**
         * @param maxDictionaryByteSize
         * @param allocator
         */
        public IntegerWriter(int maxDictionaryByteSize, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, Encoding.RLE_DICTIONARY, Encoding.OPOP_DICTIONARY, allocator);
            dictionarySections = new ArrayList<>();
            dictionarySections.add(new IntOpenHashSet());
        }

        @Override
        public BytesInput getBytes() {
            // This marks the end of a page
            // Sort the dictionary and re-encode the key
            if (!dictionarySections.get(dictionarySections.size() - 1).isEmpty()) {
                sortDictionaryThenRecode();
            }
            dictionarySections.add(new IntOpenHashSet());
            return super.getBytes();
        }

        @Override
        public void writeInteger(int v) {
            if (!intDictionaryContent.containsKey(v)) {
                // Record the newly inserted keys in this section
                dictionarySections.get(dictionarySections.size() - 1).add(v);
            }
            super.writeInteger(v);
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                if (dictionarySections.get(dictionarySections.size() - 1).isEmpty()) {
                    dictionarySections.remove(dictionarySections.size() - 1);
                }

                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);

                // Write the number of sections
                dictionaryEncoder.writeInteger(dictionarySections.size());
                // Write the number of total elements for easy space allocation when reading
                dictionaryEncoder.writeInteger(intDictionaryContent.size());
                // Write the length of each section, and the content
                dictionarySections.forEach(sec -> {
                    dictionaryEncoder.writeInteger(sec.size());
                    IntIterator ite = sec.iterator();
                    while (ite.hasNext()) {
                        dictionaryEncoder.writeInteger(ite.nextInt());
                    }
                });
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        protected void sortDictionaryThenRecode() {
            // Sort the entry in dictionary content and re-encode the entries
            int[] sorted = intDictionaryContent.keySet().toIntArray();
            Arrays.sort(sorted);
            Int2IntMap mapping = new Int2IntOpenHashMap();
            Int2IntMap newIntDictionaryContent = new Int2IntLinkedOpenHashMap();
            newIntDictionaryContent.defaultReturnValue(-1);
            for (int i = 0; i < sorted.length; i++) {
                mapping.put(intDictionaryContent.get(sorted[i]), i);
                newIntDictionaryContent.put(sorted[i], i);
            }
            intDictionaryContent = newIntDictionaryContent;

            IntList newEncodedValues = new IntList();
            IntList.IntIterator it = encodedValues.iterator();
            while (it.hasNext()) {
                newEncodedValues.add(mapping.get(it.next()));
            }
            encodedValues = newEncodedValues;
        }
    }

    public static class DoubleWriter extends DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter {

        List<DoubleSet> dictionarySections;

        /**
         * @param maxDictionaryByteSize
         * @param allocator
         */
        public DoubleWriter(int maxDictionaryByteSize, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, Encoding.RLE_DICTIONARY, Encoding.OPOP_DICTIONARY, allocator);
            dictionarySections = new ArrayList<>();
            dictionarySections.add(new DoubleOpenHashSet());
        }

        @Override
        public BytesInput getBytes() {
            // This marks the end of a page
            // Sort the dictionary and re-encode the key
            if (!dictionarySections.get(dictionarySections.size() - 1).isEmpty()) {
                sortDictionaryThenRecode();
            }
            dictionarySections.add(new DoubleOpenHashSet());
            return super.getBytes();
        }

        @Override
        public void writeDouble(double v) {
            if (!doubleDictionaryContent.containsKey(v)) {
                // Record the newly inserted keys in this section
                dictionarySections.get(dictionarySections.size() - 1).add(v);
            }
            super.writeDouble(v);
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                if (dictionarySections.get(dictionarySections.size() - 1).isEmpty()) {
                    dictionarySections.remove(dictionarySections.size() - 1);
                }

                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);

                // Write the number of sections
                dictionaryEncoder.writeInteger(dictionarySections.size());
                // Write the number of total elements for easy space allocation when reading
                dictionaryEncoder.writeInteger(doubleDictionaryContent.size());
                // Write the length of each section, and the content
                dictionarySections.forEach(sec -> {
                    dictionaryEncoder.writeInteger(sec.size());
                    DoubleIterator ite = sec.iterator();
                    while (ite.hasNext()) {
                        dictionaryEncoder.writeDouble(ite.nextDouble());
                    }
                });
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        protected void sortDictionaryThenRecode() {
            // Sort the entry in dictionary content and re-encode the entries
            double[] sorted = doubleDictionaryContent.keySet().toDoubleArray();
            Arrays.sort(sorted);
            Int2IntMap mapping = new Int2IntOpenHashMap();
            Double2IntMap newDoubleDictionaryContent = new Double2IntLinkedOpenHashMap();
            newDoubleDictionaryContent.defaultReturnValue(-1);
            for (int i = 0; i < sorted.length; i++) {
                mapping.put(doubleDictionaryContent.get(sorted[i]), i);
                newDoubleDictionaryContent.put(sorted[i], i);
            }
            doubleDictionaryContent = newDoubleDictionaryContent;

            IntList newEncodedValues = new IntList();
            IntList.IntIterator it = encodedValues.iterator();
            while (it.hasNext()) {
                newEncodedValues.add(mapping.get(it.next()));
            }
            encodedValues = newEncodedValues;
        }
    }
}
