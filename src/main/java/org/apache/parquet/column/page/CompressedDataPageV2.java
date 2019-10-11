package org.apache.parquet.column.page;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;

import static org.apache.parquet.hadoop.CodecFactory.BytesDecompressor;

/**
 * This page does not decompress data until data content is being accessed
 */
public class CompressedDataPageV2 extends DataPageV2 {

    BytesInput data;

    BytesDecompressor decompressor;

    boolean compressed = true;

    public CompressedDataPageV2(DataPageV2 dataPage, BytesDecompressor decompressor) {
        super(dataPage.getRowCount(), dataPage.getNullCount(), dataPage.getValueCount(), dataPage.getRepetitionLevels(),
                dataPage.getDefinitionLevels(), dataPage.getDataEncoding(), BytesInput.empty(), dataPage.getUncompressedSize(),
                dataPage.getStatistics(), dataPage.isCompressed());
        this.data = dataPage.getData();
        this.decompressor = decompressor;
    }

    @Override
    public BytesInput getData() {
        try {
            if (compressed) {
                this.data = decompressor.decompress(this.data, getUncompressedSize());
                compressed = false;
            }
            return this.data;
        } catch (IOException e) {
            throw new ParquetEncodingException("cannot decompress page", e);
        }
    }
}
