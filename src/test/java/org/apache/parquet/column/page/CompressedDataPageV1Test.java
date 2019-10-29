package org.apache.parquet.column.page;

import it.unimi.dsi.fastutil.bytes.Byte2BooleanArrayMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.VersionParser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.apache.parquet.TestUtils.readFooter;
import static org.junit.jupiter.api.Assertions.*;

class CompressedDataPageV1Test {

    @Test
    void getBytes() throws Exception {
        Configuration conf = new Configuration();
        // This is a file containing number from 0 to 49999
        Footer footer = readFooter("lineitem_gzip.parquet", conf);

        ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile());
        VersionParser.ParsedVersion version =
                VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
        PageReadStore rowGroup = fileReader.readNextRowGroup();
        ColumnDescriptor coldesc = footer.getParquetMetadata()
                .getFileMetaData().getSchema().getColumns().get(0);

        PageReader reader = rowGroup.getPageReader(coldesc);
        reader.readDictionaryPage();
        DataPage page = reader.readPage();
        BytesInput data = page.accept(new DataPage.Visitor<BytesInput>() {

            @Override
            public BytesInput visit(DataPageV1 dataPageV1) {
                return dataPageV1.getBytes();
            }

            @Override
            public BytesInput visit(DataPageV2 dataPageV2) {
                return dataPageV2.getData();
            }
        });

        ByteBuffer buffer = data.toByteBuffer();
        return;
    }
}