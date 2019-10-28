package org.apache.parquet.column.values.dictionary;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;

/**
 * PageDictionary allows each page to use a different dictionary view.
 */
public class OnePassOrderPreservingDictionary {

    public static class BinaryDictionary extends Dictionary {

        private Binary[] dictionaryContent;
        private int[] pageSizes;
        int pageIndex = 0;
        int entrySize = 0;

        public BinaryDictionary(DictionaryPage dictPage) {
            super(dictPage.getEncoding());
            try {
                final ByteBuffer byteBuffer = dictPage.getBytes().toByteBuffer();
                int offset = 0;
                int numPages = BytesUtils.readIntLittleEndian(byteBuffer, offset);
                offset += 4;
                int numEntries = BytesUtils.readIntLittleEndian(byteBuffer, offset);
                offset += 4;
                dictionaryContent = new Binary[numEntries];
                pageSizes = new int[numPages];
                int entryCounter = 0;
                for (int i = 0; i < numPages; i++) {
                    pageSizes[i] = BytesUtils.readIntLittleEndian(byteBuffer, offset);
                    offset += 4;
                    for (int j = 0; j < pageSizes[i]; j++) {
                        int len = readIntLittleEndian(byteBuffer, offset);
                        offset += 4;
                        dictionaryContent[entryCounter++] = Binary.fromConstantByteBuffer(byteBuffer, offset, len);
                        offset += len;
                    }
                }
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }

        @Override
        public int getMaxId() {
            return dictionaryContent.length - 1;
        }

        @Override
        public void nextPage() {
            entrySize += pageSizes[pageIndex++];
            Arrays.sort(dictionaryContent, 0, entrySize);
        }

        @Override
        public Binary decodeToBinary(int id) {
            return dictionaryContent[id];
        }

        @Override
        public int encodeBinary(Binary value) {
            int low = 0;
            int high = entrySize;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                Binary midVal = dictionaryContent[mid];

                if (midVal.compareTo(value) < 0)
                    low = mid + 1;
                else if (midVal.compareTo(value) > 0)
                    high = mid - 1;
                else
                    return mid; // key found
            }
            return -(low + 1);  // key not found.
        }
    }


}
