package org.apache.parquet.column.impl;

import edu.uchicago.cs.db.parquet.NonePrimitiveConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.parquet.TestUtils.readFooter;
import static org.junit.jupiter.api.Assertions.*;

class SkippingColumnReaderImplTest {

    @Test
    void normalRead() throws Exception {
        Configuration conf = new Configuration();
        // This is a file containing number from 0 to 49999
        Footer footer = readFooter("multipage_autoinc.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(0);

        SkippingColumnReaderImpl columnReader =
                new SkippingColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version);
        int[] values = new int[(int) rowGroup.getRowCount()];

        for (int i = 0; i < values.length; i++) {
            assertEquals(i + 1, columnReader.getReadValues());
            values[i] = columnReader.getInteger();
            columnReader.consume();
            assertEquals(i, values[i], String.valueOf(i));
        }
    }


    @Test
    void pageSkipRead() throws Exception {
        Configuration conf = new Configuration();
        // This is a file containing number from 0 to 49999
        Footer footer = readFooter("multipage_autoinc.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(0);

        Predicate<Statistics<?>> pageFilter = stat -> {
            IntStatistics intstat = (IntStatistics) stat;
            // Skip the pages in the middle
            return intstat.getMin() == 0 || intstat.getMin() > 5000;
        };

        SkippingColumnReaderImpl columnReader =
                new SkippingColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version, pageFilter, null);

        List<Integer> buffer = new ArrayList<>();

        while (columnReader.getReadValues() <= rowGroup.getRowCount()) {
            buffer.add(columnReader.getInteger());
            columnReader.consume();
        }

        // Read the file again and compare
        ParquetFileReader fileReader2 = ParquetFileReader.open(conf, footer.getFile());
        PageReadStore rowGroup2 = fileReader2.readNextRowGroup();
        PageReader pages = rowGroup2.getPageReader(coldesc);
        DataPage page;
        long counter = 0;
        while ((page = pages.readPage()) != null) {
            boolean accept = page.accept(new DataPage.Visitor<Boolean>() {
                @Override
                public Boolean visit(DataPageV1 dataPageV1) {
                    return pageFilter.test(dataPageV1.getStatistics());
                }

                @Override
                public Boolean visit(DataPageV2 dataPageV2) {
                    return pageFilter.test(dataPageV2.getStatistics());
                }
            });
            if (accept) {
                counter += page.getValueCount();
            }
        }
        assertEquals(counter, buffer.size());
    }

    @Test
    void recordSkipRead() throws Exception {
        Configuration conf = new Configuration();
        // This is a file containing number from 0 to 49999
        Footer footer = readFooter("multipage_autoinc.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(0);

        ForwardIterator rowFilter = new ForwardIterator() {

            long counter = 0;

            @Override
            public void startfrom(long pos) {
                counter = pos;
            }

            @Override
            public long nextLong() {
                if (counter / 13 * 13 == counter)
                    return counter;
                return (counter / 13) * 13 + 13;
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        };

        SkippingColumnReaderImpl columnReader =
                new SkippingColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version, stat -> true, rowFilter);

        List<Integer> buffer = new ArrayList<>();

        while (columnReader.getReadValues() <= rowGroup.getRowCount()) {
            buffer.add(columnReader.getInteger());
            columnReader.consume();
        }
        for (int i = 0; i < buffer.size(); i++) {
            assertEquals(i * 13, buffer.get(i));
        }
    }

    @Test
    void doubleFilterRead() throws Exception {
        Configuration conf = new Configuration();
        // This is a file containing number from 0 to 49999
        Footer footer = readFooter("multipage_autoinc.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(0);

        ParquetFileReader fileReader2 = ParquetFileReader.open(conf, footer.getFile());
        PageReadStore rowGroup2 = fileReader2.readNextRowGroup();
        PageReader reader = rowGroup2.getPageReader(coldesc);


        Predicate<Statistics<?>> pageFilter = new Predicate<Statistics<?>>() {
            @Override
            public boolean test(Statistics<?> statistics) {
                IntStatistics ints = (IntStatistics) statistics;
                int pageIndex = ints.getMax() / 1250;
                return pageIndex % 3 == 1;
            }
        };

        List<DataPage> pages = new ArrayList<>();
        DataPage dp = null;
        while ((dp = reader.readPage()) != null) {
            if (dp.accept(new DataPage.Visitor<Boolean>() {
                @Override
                public Boolean visit(DataPageV1 dataPageV1) {
                    return pageFilter.test(dataPageV1.getStatistics());
                }

                @Override
                public Boolean visit(DataPageV2 dataPageV2) {
                    return pageFilter.test(dataPageV2.getStatistics());
                }
            }))
                pages.add(dp);
        }

        ForwardIterator rowFilter = new ForwardIterator() {

            long counter = 0;

            @Override
            public void startfrom(long pos) {
                counter = pos;
            }

            @Override
            public long nextLong() {
                return ((counter / 11) * 11) + 11;
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        };

        SkippingColumnReaderImpl columnReader =
                new SkippingColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version, pageFilter, rowFilter);

        List<Integer> buffer = new ArrayList<>();

        while (columnReader.getReadValues() <= rowGroup.getRowCount()) {
            buffer.add(columnReader.getInteger());
            columnReader.consume();
        }

        List<Integer> expect = new ArrayList<>();

        for (DataPage page : pages) {
            page.accept(new DataPage.Visitor<Object>() {
                @Override
                public Object visit(DataPageV2 dataPageV2) {
                    IntStatistics ints = (IntStatistics) dataPageV2.getStatistics();
                    for (int i = ints.getMin(); i <= ints.getMax(); i++) {
                        if (i / 11 * 11 == i) {
                            expect.add(i);
                        }
                    }

                    return null;
                }

                @Override
                public Object visit(DataPageV1 dataPageV1) {
                    IntStatistics ints = (IntStatistics) dataPageV1.getStatistics();
                    for (int i = ints.getMin(); i <= ints.getMax(); i++) {
                        if (i / 11 * 11 == i) {
                            expect.add(i);
                        }
                    }

                    return null;
                }
            });
        }
        expect.remove(0);
        expect.remove(new Integer(41283));


        assertEquals(expect.size(), buffer.size());

        for (int i = 0; i < buffer.size(); i++) {
            assertEquals(expect.get(i), buffer.get(i));
        }
    }
}