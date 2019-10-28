package org.apache.parquet.column.values.dictionary;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;

public class EmptyDictionary extends Dictionary {

    public static EmptyDictionary INSTANCE = new EmptyDictionary();

    public EmptyDictionary() {
        super(Encoding.PLAIN);
    }

    @Override
    public int getMaxId() {
        return 0;
    }
}
