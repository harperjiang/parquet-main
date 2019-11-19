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

package edu.uchicago.cs.db.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CharSequenceWriteSupport extends WriteSupport<CharSequence[]> {

    Logger logger = LoggerFactory.getLogger(getClass());
    protected MessageType schema;
    protected RecordConsumer recordConsumer;
    protected List<ColumnDescriptor> cols;

    public CharSequenceWriteSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer r) {
        recordConsumer = r;
    }

    @Override
    public void write(CharSequence[] values) {
        if (values.length != cols.size()) {
            throw new ParquetEncodingException("Invalid input data. Expecting " + cols.size() + " columns. Input had "
                    + values.length + " columns (" + cols + ") : " + values);
        }

        recordConsumer.startMessage();
        for (int i = 0; i < cols.size(); ++i) {
            CharSequence val = values[i];
            // val.length() == 0 indicates a NULL value.

            if (val.length() > 0) {
                recordConsumer.startField(cols.get(i).getPath()[0], i);
                try {
                    switch (cols.get(i).getType()) {
                        case BOOLEAN:
                            recordConsumer.addBoolean("true".equals(val) || "yes".equals(val) || "1".equals(val));
                            break;
                        case FLOAT:
                        case INT64:
                            throw new UnsupportedOperationException();
                        case DOUBLE:
                            recordConsumer.addDouble(parseDouble(val));
                            break;
                        case INT32:
                            recordConsumer.addInteger(parseInt(val));
                            break;
                        case INT96:
                            recordConsumer.addInteger(parseHexInt(val));
                        case BINARY:
                            recordConsumer.addBinary(charSeqToBinary(val));
                            break;
                        default:
                            throw new ParquetEncodingException("Unsupported column type: " + cols.get(i).getType());
                    }
                } catch (Exception e) {
                    logger.warn("Malformated data encountered and skipping:" + val, e);
                }
                recordConsumer.endField(cols.get(i).getPath()[0], i);
            }
        }
        recordConsumer.endMessage();
    }


    @Override
    public FinalizedWriteContext finalizeWrite() {
        Map<String, String> extrameta = new HashMap<>();
        // Write encoding context information

        EncContext.context.get().entrySet().forEach((Map.Entry<String, Object[]> entry) -> {
            extrameta.put(entry.getKey() + ".0", entry.getValue()[0].toString());
            extrameta.put(entry.getKey() + ".1", entry.getValue()[1].toString());
        });

        return new FinalizedWriteContext(extrameta);
    }

    protected static double parseDouble(CharSequence input) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    static int[] INTMUL = new int[]{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

    protected static int parseInt(CharSequence input) {
        int value = 0;
        int length = input.length();
        for (int i = 0; i < input.length(); i++) {
            value += INTMUL[length - 1 - i] * (input.charAt(i) - '0');
        }
        return value;
    }

    static int[] HEXVAL = new int[128];

    static {
        for (int i = 0; i < 10; i++) {
            HEXVAL['0' + i] = i;
        }
        for (int i = 0; i < 6; i++) {
            HEXVAL['A' + i] = 10 + i;
            HEXVAL['a' + i] = 10 + i;
        }
    }

    protected static int parseHexInt(CharSequence input) {
        int value = 0;
        int length = input.length();
        for (int i = 0; i < input.length(); i++) {
            value += HEXVAL[input.charAt(i)] << ((length - 1 - i) * 4);
        }
        return value;
    }

    private Binary charSeqToBinary(Object value) {
        return Binary.fromCharSequence(value.toString());
    }
}
