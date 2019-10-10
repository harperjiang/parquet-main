/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */
package org.apache.parquet.hadoop;

import org.apache.parquet.Ints;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.CodecFactory.BytesDecompressor;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * TODO: should this actually be called RowGroupImpl or something?
 * The name is kind of confusing since it references three different "entities"
 * in our format: columns, chunks, and pages
 */
public class ColumnChunkPageReadStore implements PageReadStore, DictionaryPageReadStore {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkPageReadStore.class);

    /**
     * PageReader for a single column chunk. A column chunk contains
     * several pages, which are yielded one by one in order.
     * <p>
     * This implementation is provided with a list of pages, each of which
     * is decompressed and passed through.
     */
    public static final class ColumnChunkPageReader implements PageReader {

        private final BytesDecompressor decompressor;
        private final long valueCount;
        private final List<DataPage> compressedPages;
        private final DictionaryPage compressedDictionaryPage;

        // Parquet predicate class
        private FilterPredicate filterPredicate = null;

        public FilterPredicate getFilterPredicate() {
            return filterPredicate;
        }

        public void setFilterPredicate(FilterPredicate filterPredicate) {
            this.filterPredicate = filterPredicate;
        }

        /**
         * Counter of how many records have been skipped
         */
        private long skipped;

        @Override
        public void setToSkip(long toSkip) {
            this.toSkip = toSkip;
        }

        /**
         * Passed by reader, indicating how many records to skip
         */
        private long toSkip;

        private long startpos;

        public long getStartpos() {
            return startpos;
        }

        public void setStartpos(long startpos) {
            this.startpos = startpos;
        }

        ColumnChunkPageReader(BytesDecompressor decompressor, List<DataPage> compressedPages, DictionaryPage compressedDictionaryPage) {
            this.decompressor = decompressor;
            this.compressedPages = new LinkedList<DataPage>(compressedPages);
            this.compressedDictionaryPage = compressedDictionaryPage;
            long count = 0;
            long starpos = 0;
            for (DataPage p : compressedPages) {
                count += p.getValueCount();
            }
            this.valueCount = count;
        }

        @Override
        public long getTotalValueCount() {
            return valueCount;
        }

        @Override
        public DataPage readPage() {
            if (compressedPages.isEmpty()) {
                return null;
            }
            while (!compressedPages.isEmpty()) {
                DataPage compressedPage = compressedPages.remove(0);
                //DataPageV1 compressedPage = (DataPageV1)compressedPages.remove(0);
                //System.out.println(compressedPage.getStatistics().toString());

                if (toSkip > compressedPage.getValueCount()) {
                    this.skipped += compressedPage.getValueCount();
                    //System.out.println("consume skipped:" + this.getSkipped());
                    startpos += compressedPage.getValueCount();
                    toSkip -= compressedPage.getValueCount();
                } else {
                    return compressedPage.accept(new DataPage.Visitor<DataPage>() {
                        @Override
                        public DataPage visit(DataPageV1 dataPageV1) {
                            try {
                                //System.out.println(dataPageV1.getStatistics().toString());
                                return new DataPageV1(
                                        decompressor.decompress(dataPageV1.getBytes(), dataPageV1.getUncompressedSize()),
                                        dataPageV1.getValueCount(),
                                        dataPageV1.getUncompressedSize(),
                                        dataPageV1.getStatistics(),
                                        dataPageV1.getRlEncoding(),
                                        dataPageV1.getDlEncoding(),
                                        dataPageV1.getValueEncoding());
                            } catch (IOException e) {
                                throw new ParquetDecodingException("could not decompress page", e);
                            }
                        }

                        @Override
                        public DataPage visit(DataPageV2 dataPageV2) {
                            if (!dataPageV2.isCompressed()) {
                                return dataPageV2;
                            }
                            try {
                                int uncompressedSize = Ints.checkedCast(
                                        dataPageV2.getUncompressedSize()
                                                - dataPageV2.getDefinitionLevels().size()
                                                - dataPageV2.getRepetitionLevels().size());
                                return DataPageV2.uncompressed(
                                        dataPageV2.getRowCount(),
                                        dataPageV2.getNullCount(),
                                        dataPageV2.getValueCount(),
                                        dataPageV2.getRepetitionLevels(),
                                        dataPageV2.getDefinitionLevels(),
                                        dataPageV2.getDataEncoding(),
                                        decompressor.decompress(dataPageV2.getData(), uncompressedSize),
                                        dataPageV2.getStatistics()
                                );
                            } catch (IOException e) {
                                throw new ParquetDecodingException("could not decompress page", e);
                            }
                        }
                    });
                }
            }
            LOG.debug("final skipped:" + this.getSkipped());
            return null;
        }

        @Override
        public DictionaryPage readDictionaryPage() {
            if (compressedDictionaryPage == null) {
                return null;
            }
            try {
                return new DictionaryPage(
                        decompressor.decompress(compressedDictionaryPage.getBytes(), compressedDictionaryPage.getUncompressedSize()),
                        compressedDictionaryPage.getDictionarySize(),
                        compressedDictionaryPage.getEncoding());
            } catch (IOException e) {
                throw new ParquetDecodingException("Could not decompress dictionary page", e);
            }
        }

        public long getSkipped() {
            return skipped;
        }

        public long checkSkipped() {
            long ret = skipped;
            skipped = 0;
            return ret;
        }

        public long getCompressedSizeInBytes() {
            long size = 0;
            for (DataPage page : compressedPages) {
                size += page.getCompressedSize();
            }
            return size;
        }
    }

    private final Map<ColumnDescriptor, ColumnChunkPageReader> readers = new HashMap<ColumnDescriptor, ColumnChunkPageReader>();
    private final long rowCount;

    public ColumnChunkPageReadStore(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public PageReader getPageReader(ColumnDescriptor path) {
        if (!readers.containsKey(path)) {
            throw new IllegalArgumentException(path + " is not in the store: " + readers.keySet() + " " + rowCount);
        }
        return readers.get(path);
    }

    @Override
    public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
        return readers.get(descriptor).readDictionaryPage();
    }

    void addColumn(ColumnDescriptor path, ColumnChunkPageReader reader) {
        if (readers.put(path, reader) != null) {
            throw new RuntimeException(path + " was added twice");
        }
    }


}
