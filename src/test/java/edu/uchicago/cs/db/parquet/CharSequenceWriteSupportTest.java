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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CharSequenceWriteSupportTest {

    @Test
    void parseInt() {
        assertEquals(1414, CharSequenceWriteSupport.parseInt("1414"));
    }

    @Test
    void parseHexInt() {
        assertEquals(0x3248A, CharSequenceWriteSupport.parseHexInt("3248A"));
        assertEquals(0x3248A, CharSequenceWriteSupport.parseHexInt("3248a"));
    }
}