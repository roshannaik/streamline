/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package com.hortonworks.streamline.streams.runtime.storm.bolt.query;


import org.apache.storm.tuple.Tuple;

abstract class JoinComparator {
    private final String field1; // from 1st stream
    private final String field2; // from 2nd stream

    FieldSelector fieldSelector1;
    FieldSelector fieldSelector2;

    public JoinComparator(String fieldSelector1, String fieldSelector2) {
        this.field1 = fieldSelector1;
        this.field2 = fieldSelector2;
    }

    public void setStreamKind(RealtimeJoinBolt.StreamKind streamKind) {
        this.fieldSelector1 = new FieldSelector(field1, streamKind);
        this.fieldSelector2 = new FieldSelector(field2, streamKind);
    }

    public FieldSelector getFieldSelector1() {
        return fieldSelector1;
    }

    public FieldSelector getFieldSelector2() {
        return fieldSelector2;
    }

    public abstract boolean compare(Tuple t1, Tuple t2) throws InvalidTuple;

}
