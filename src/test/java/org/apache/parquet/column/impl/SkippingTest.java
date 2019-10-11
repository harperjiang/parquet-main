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

package org.apache.parquet.column.impl;

import edu.uchicago.cs.db.parquet.NonePrimitiveConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.apache.parquet.TestUtils.readFooter;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SkippingTest {

    public void testOnPhone() throws Exception {
        long[] testIndices = new long[]{37504, 37506, 37508};
        Configuration conf = new Configuration();
        Footer footer = readFooter("phone.subattr.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(2);

        ColumnReaderImpl columnReader =
                new ColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version);
        int[] values = new int[(int) rowGroup.getRowCount()];

        for (int i = 0; i < values.length; i++) {
            values[i] = columnReader.getInteger();
            columnReader.consume();
        }

        fileReader = ParquetFileReader.open(conf, footer.getFile());
        rowGroup = fileReader.readNextRowGroup();

        columnReader = new ColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                new NonePrimitiveConverter(), version);

        columnReader.consumeTo(5);

//        columnReader.consumeTo(5);
//        for (int i = 0; i < testIndices.length; i++) {
//            columnReader.consumeTo(testIndices[i]);
//            System.out.println(i);
//            System.out.println(values[(int) testIndices[i]]);
//            assertEquals(values[(int) testIndices[i]], columnReader.getInteger());
//        }
    }

    @Test
    public void testPageSkippingOnPlain() throws Exception {
        Configuration conf = new Configuration();
        Footer footer = readFooter("multipage_plain.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(0);

        ColumnReaderImpl columnReader =
                new ColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version);
        int[] values = new int[(int) rowGroup.getRowCount()];

        for (int i = 0; i < values.length; i++) {
            values[i] = columnReader.getInteger();
            columnReader.consume();
        }

        fileReader = ParquetFileReader.open(conf, footer.getFile());
        rowGroup = fileReader.readNextRowGroup();

        columnReader = new ColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                new NonePrimitiveConverter(), version);

        Random rand = new Random(System.currentTimeMillis());
        int counter = Math.abs(rand.nextInt() % 350);
        while (counter < values.length) {
            columnReader.consumeTo(counter);
//            System.out.println(counter);
            assertEquals(values[counter], columnReader.getInteger());
            counter += Math.abs(rand.nextInt() % 350 + 1);
        }
    }

    @Test
    public void testPageSkippingOnDelta() throws Exception {
        Configuration conf = new Configuration();
        Footer footer = readFooter("multipage_delta.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(0);

        ColumnReaderImpl columnReader =
                new ColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version);
        int[] values = new int[(int) rowGroup.getRowCount()];

        for (int i = 0; i < values.length; i++) {
            values[i] = columnReader.getInteger();
            columnReader.consume();
        }

        fileReader = ParquetFileReader.open(conf, footer.getFile());
        rowGroup = fileReader.readNextRowGroup();

        columnReader = new ColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                new NonePrimitiveConverter(), version);

        Random rand = new Random(System.currentTimeMillis());
        int counter = Math.abs(rand.nextInt() % 350);
        while (counter < values.length) {
            columnReader.consumeTo(counter);
//            System.out.println(counter);
            assertEquals(values[counter], columnReader.getInteger());
            counter += Math.abs(rand.nextInt() % 350 + 1);
        }
    }

    @Test
    public void testPageSkippingOnRle() throws Exception {
        Configuration conf = new Configuration();
        Footer footer = readFooter("multipage_rle.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(0);

        ColumnReaderImpl columnReader =
                new ColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version);
        int[] values = new int[(int) rowGroup.getRowCount()];

        for (int i = 0; i < values.length; i++) {
            values[i] = columnReader.getInteger();
            columnReader.consume();
        }

        fileReader = ParquetFileReader.open(conf, footer.getFile());
        rowGroup = fileReader.readNextRowGroup();

        columnReader = new ColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                new NonePrimitiveConverter(), version);

        Random rand = new Random(System.currentTimeMillis());
        int counter = Math.abs(rand.nextInt() % 350);
        while (counter < values.length) {
            columnReader.consumeTo(counter);
//            System.out.println(counter);
            assertEquals(values[counter], columnReader.getInteger());
            counter += Math.abs(rand.nextInt() % 350 + 1);
        }
    }

    @Test
    public void testSkippingWithNullValue() throws Exception {
        Configuration conf = new Configuration();
        Footer footer = readFooter("withnull.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();

        MessageType schema = footer.getParquetMetadata().getFileMetaData().getSchema();
        ColumnDescriptor coldesc = schema.getColumns().get(0);
        ColumnReaderImpl reader = new ColumnReaderImpl(coldesc,
                rowGroup.getPageReader(coldesc), new NonePrimitiveConverter(), version);

        Integer[] values = new Integer[(int) reader.getTotalValueCount()];
        for (int i = 0; i < values.length; i++) {
            if (reader.getCurrentDefinitionLevel() > 0) {
                values[i] = reader.getInteger();
            } else {
                values[i] = null;
            }
            reader.consume();
        }

        for (int round = 0; round < 1000; round++) {
            fileReader = ParquetFileReader.open(conf, footer.getFile());
            rowGroup = fileReader.readNextRowGroup();
            reader = new ColumnReaderImpl(coldesc,
                    rowGroup.getPageReader(coldesc), new NonePrimitiveConverter(), version);

            Random rand = new Random(System.currentTimeMillis());
            int counter = Math.abs(rand.nextInt() % 50) + 1;
            while (counter < values.length) {
                if (values[counter] != null) {
//                    System.out.println(counter);
                    reader.consumeTo(counter);
                    assertEquals(values[counter].intValue(), reader.getInteger());
                }
                counter += Math.abs(rand.nextInt() % 50) + 1;
            }
        }
    }

    @Test
    public void testSkippingWithNullValueDebug() throws Exception {
        Configuration conf = new Configuration();
        Footer footer = readFooter("withnull.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();

        MessageType schema = footer.getParquetMetadata().getFileMetaData().getSchema();
        ColumnDescriptor coldesc = schema.getColumns().get(0);
        ColumnReaderImpl reader = new ColumnReaderImpl(coldesc,
                rowGroup.getPageReader(coldesc), new NonePrimitiveConverter(), version);

        Integer[] values = new Integer[(int) reader.getTotalValueCount()];
        for (int i = 0; i < values.length; i++) {
            if (reader.getCurrentDefinitionLevel() > 0) {
                values[i] = reader.getInteger();
            } else {
                values[i] = null;
            }
            reader.consume();
        }

        fileReader = ParquetFileReader.open(conf, footer.getFile());
        rowGroup = fileReader.readNextRowGroup();
        reader = new ColumnReaderImpl(coldesc,
                rowGroup.getPageReader(coldesc), new NonePrimitiveConverter(), version);


        reader.consumeTo(1);
        assertEquals(values[1].intValue(), reader.getInteger());
        reader.consumeTo(78);
        assertEquals(values[78].intValue(), reader.getInteger());
    }


}
