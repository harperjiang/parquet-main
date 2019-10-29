/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column;

import edu.uchicago.cs.db.common.functional.IntDoubleConsumer;
import edu.uchicago.cs.db.common.functional.IntIntConsumer;
import edu.uchicago.cs.db.common.functional.IntObjectConsumer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.io.api.Binary;

import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * a dictionary to decode dictionary based encodings
 *
 * @author Julien Le Dem
 */
public abstract class Dictionary {

    private final Encoding encoding;

    public Dictionary(Encoding encoding) {
        this.encoding = encoding;
    }

    public Encoding getEncoding() {
        return encoding;
    }

    /**
     * @param skip true if the next page data is not needed. E.g., page is skipped
     * @return true if there is new dictionary entry in the page
     */
    public boolean nextPage(boolean skip) {
        return false;
    }

    public abstract int getMaxId();

    public int encodeInt(int value) {
        throw new UnsupportedOperationException();
    }

    public int encodeBinary(Binary value) {
        throw new UnsupportedOperationException();
    }

    public int encodeDouble(double value) {
        throw new UnsupportedOperationException();
    }

    public Binary decodeToBinary(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    public int decodeToInt(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    public long decodeToLong(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    public float decodeToFloat(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    public double decodeToDouble(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    public boolean decodeToBoolean(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    public void access(IntObjectConsumer<Binary> consumer) {
        throw new UnsupportedOperationException();
    }

    public void access(IntDoubleConsumer consumer) {
        throw new UnsupportedOperationException();
    }

    public void access(IntIntConsumer consumer) {
        throw new UnsupportedOperationException();
    }

    public IntList filterInt(IntPredicate pred) {
        IntList result = new IntArrayList();
        access((int index, int value) -> {
            if (pred.test(value))
                result.add(index);
        });
        return result;
    }

    public IntList filterDouble(DoublePredicate pred) {
        IntList result = new IntArrayList();
        access((int index, double value) -> {
            if (pred.test(value))
                result.add(index);
        });
        return result;
    }

    public IntList filterBinary(Predicate<Binary> pred) {
        IntList result = new IntArrayList();
        access((int index, Binary value) -> {
            if (pred.test(value))
                result.add(index);
        });
        return result;
    }
}
