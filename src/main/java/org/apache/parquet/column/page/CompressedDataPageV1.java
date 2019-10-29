package org.apache.parquet.column.page;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.parquet.hadoop.CodecFactory.BytesDecompressor;

/**
 * This page does not decompress data until the content is being accessed
 */
public class CompressedDataPageV1 extends DataPageV1 {

    BytesInput compressedData;

    BytesDecompressor decompressor;

    boolean compressed = true;

    public CompressedDataPageV1(DataPageV1 dataPage, BytesDecompressor decompressor) {
        super(BytesInput.empty(), dataPage.getValueCount(), dataPage.getUncompressedSize(), dataPage.getStatistics(),
                dataPage.getRlEncoding(), dataPage.getDlEncoding(), dataPage.getValueEncoding());
        this.compressedData = dataPage.getBytes();
        this.decompressor = decompressor;
    }

    @Override
    public BytesInput getBytes() {
        try {
            if (compressed) {
                // Decompress to DirectByteBuffer
                ByteBuffer directBuffer = new DirectByteBufferAllocator().allocate(this.getUncompressedSize());
                this.decompressor.decompress(this.compressedData.toByteBuffer(), getCompressedSize(),
                        directBuffer, getUncompressedSize());
                compressedData = BytesInput.from(directBuffer, 0, getUncompressedSize());
                compressed = false;
            }
            return compressedData;
        } catch (IOException e) {
            throw new ParquetEncodingException("cannot decompress page", e);
        }
    }
}
