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

package org.apache.parquet;

import edu.uchicago.cs.db.parquet.NonePrimitiveConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.SkippingColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class TestUtils {

    public static Footer readFooter(String fileName, Configuration conf) throws Exception {
        File tableFile;
        try {
            tableFile = new File(Thread.currentThread().getContextClassLoader()
                    .getResource(fileName).toURI());
        } catch (Exception e) {
            tableFile = new File(fileName);
        }
        Path path = new Path(tableFile.getAbsolutePath());
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader
                .readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        Footer footer = footers.get(0);
        return footer;
    }

    public static int[] readIntBlock(String fileName, int blockId, int columnId) throws Exception {
        Configuration conf = new Configuration();
        Footer footer = readFooter(fileName, conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        for (int i = 0; i < blockId; i++) {
            fileReader.skipNextRowGroup();
        }
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(columnId);

        SkippingColumnReaderImpl columnReader =
                new SkippingColumnReaderImpl(coldesc, rowGroup.getPageReader(coldesc),
                        new NonePrimitiveConverter(), version);
        int[] values = new int[(int) rowGroup.getRowCount()];

        for (int i = 0; i < values.length; i++) {
            values[i] = columnReader.getInteger();
            columnReader.consume();
        }
        return values;
    }
}
