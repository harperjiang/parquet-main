package org.apache.parquet.column.values.vbp;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * See <code>VarlenBitPackingValuesWriter</code>
 */
public class VarlenBitPackingValuesReader extends ValuesReader {

    private int nextOffset;
    private int valueRemain;

    private int currentGroupRemain;
    private int currentBitWidth;
    private int currentGroupBase;
    private BytePacker packer;

    private byte[] packedBuffer = new byte[128];
    private int[] unpackBuffer = new int[32];
    // The offset of unpack buffer in the 512 number group
    private int bufferPointer;
    private int bufferSize;

    private ByteBufferInputStream input;

    public VarlenBitPackingValuesReader() {

    }

    @Override
    public void initFromPage(int valueCount, ByteBuffer page, int offset) throws IOException {
        this.valueRemain = valueCount;
        input = new ByteBufferInputStream(page, offset, page.limit() - offset);

        int length = BytesUtils.readIntLittleEndian(input);

        nextOffset = offset + 8 + length;
        readNextGroupHeader();
        readNext32Number();
    }


    void readNextGroupHeader() {
        try {
            currentBitWidth = input.read();
            currentGroupRemain = Math.min(512, valueRemain);
            currentGroupBase = BytesUtils.readZigZagVarInt(input);
            packer = Packer.LITTLE_ENDIAN.newBytePacker(currentBitWidth);
        } catch (IOException e) {
            throw new ParquetEncodingException("can not read block", e);
        }
    }

    void readNext32Number() {
        try {
            input.read(packedBuffer, 0, currentBitWidth * 4);
            packer.unpack32Values(packedBuffer, 0, unpackBuffer, 0);
            bufferSize = Math.min(currentGroupRemain, 32);
            currentGroupRemain -= bufferSize;

            bufferPointer = 0;
        } catch (IOException e) {
            throw new ParquetEncodingException("can not read stripe", e);
        }
    }

    @Override
    public void skip() {
        readInteger();
    }

    @Override
    public void skip(long numRecords) {
        try {
            int remain = (int) numRecords;
            // Still in current buffer
            if (bufferPointer + numRecords < bufferSize) {
                bufferPointer += numRecords;
            } else {
                remain -= bufferSize - bufferPointer;
                if (remain < currentGroupRemain) {
                    // Still within current group
                    // Use up current buffer
                    // Skip 32 num groups
                    int group32toSkip = (remain >> 5);
                    input.skip((group32toSkip * currentBitWidth) << 2);
                    currentGroupRemain -= group32toSkip * 32;

                    readNext32Number();
                    bufferPointer = remain & 0x1F;
                } else { // Calculate the target group and stripe
                    remain -= currentGroupRemain;
                    // Skip the unread data in current group
                    input.skip((((currentGroupRemain >> 5) << 5) * currentBitWidth) >> 3);
                    while (remain >= 512) {
                        // skip the whole group
                        currentBitWidth = input.read();
                        currentGroupBase = BytesUtils.readZigZagVarInt(input);
                        input.skip(currentBitWidth << 6);
                        remain -= 512;
                    }
                    readNextGroupHeader();
                    while (remain > 32) {
                        input.skip(4 * currentBitWidth);
                        currentGroupRemain-=32;
                        remain -= 32;
                    }
                    readNext32Number();
                    bufferPointer = remain;
                }
            }
            this.valueRemain -= numRecords;
        } catch (IOException e) {
            throw new ParquetEncodingException("can not skip records", e);
        }
    }

    @Override
    public int readInteger() {
        if (bufferPointer == bufferSize || bufferSize == 0) {
            if (currentGroupRemain == 0) {
                readNextGroupHeader();
                readNext32Number();
            } else {
                readNext32Number();
            }
        }
        valueRemain--;
        return currentGroupBase + unpackBuffer[bufferPointer++];
    }

    @Override
    public int getNextOffset() {
        return nextOffset;
    }
}
