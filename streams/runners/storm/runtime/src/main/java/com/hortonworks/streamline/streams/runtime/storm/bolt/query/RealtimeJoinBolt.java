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

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.Duration;


enum JoinType {INNER, LEFT, RIGHT, OUTER}

public class RealtimeJoinBolt extends BaseRichBolt  {
    private static final Logger LOG = LoggerFactory.getLogger(RealtimeJoinBolt.class);
    final static String EVENT_PREFIX = StreamlineEvent.STREAMLINE_EVENT + ".";

    private LinkedHashMap<String, JoinInfo> joinInfos = new LinkedHashMap<>(10);
    protected FieldSelector[] outputFields = null;   // specified via bolt.select() ... used in declaring Output fields
    private boolean streamLineProjection = false; // NOTE: Streamline Specific
    private String outputStream;    // output stream name

    private OutputCollector collector;

    private int streamCount=-1;

    public enum StreamKind {
        STREAM(0), SOURCE(1);
        int value;

        StreamKind(int value) {
            this.value = value;
        }

        // Returns either the source component Id or the stream Id for the tuple
        String getStreamId(Tuple ti) {
            switch (value) {
                case 0:
                    return ti.getSourceStreamId();
                case 1:
                    return ti.getSourceComponent();
                default:
                    throw new RuntimeException(value + " is unexpected");
            }
        }

    }

    // Indicates if we are using streamId or source component name to distinguish incoming tuple streams
    protected final StreamKind streamKind;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        String[] outputFieldNames = new String[outputFields.length];
        for( int i=0; i<outputFields.length; ++i ) {
            outputFieldNames[i] = outputFields[i].outputName ;
        }
        if (outputStream !=null) {
            declarer.declareStream(outputStream, new Fields(outputFieldNames));
        } else {
            declarer.declare(new Fields(outputFieldNames));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * Constructor
     * @param streamKind   Specifies whether we should use stream id or source component id as stream name.
     */
    public RealtimeJoinBolt(StreamKind streamKind) {
        this.streamKind = streamKind;
    }

    // NOTE: streamline specific
    /**
     * Calls  RealtimeJoinBolt(StreamKind.STREAM)
     */
    public RealtimeJoinBolt() {
        this(StreamKind.STREAM);
    }

    public RealtimeJoinBolt from(String stream, int retentionCount, boolean dropOlderDuplicates) {
        if (streamCount!=-1)
            throw new IllegalArgumentException("from() method can be called only once.");
        ++streamCount;

        this.joinInfos.put(stream, new JoinInfo(null, null, retentionCount, dropOlderDuplicates) );
        return this;
    }

    public RealtimeJoinBolt from(String stream, Duration retentionTime, boolean dropOlderDuplicates) {
        if (streamCount!=-1)
            throw new IllegalArgumentException("from() method can be called only once.");
        ++streamCount;
        this.joinInfos.put(stream, new JoinInfo(null, retentionTime.toMillis(), null, dropOlderDuplicates) );
        return this;
    }


    // INNER JOINS
    public RealtimeJoinBolt innerJoin(String stream, int retentionCount, boolean dropOlderDuplicates, JoinComparator... comparators) {
        return joinHelperCountRetention(JoinType.INNER, stream, retentionCount, dropOlderDuplicates, comparators);
    }

    public RealtimeJoinBolt innerJoin(String stream, Duration retentionTime, boolean dropOlderDuplicates, JoinComparator... comparators) {
        return joinHelperTimeRetention(JoinType.INNER, stream, retentionTime, dropOlderDuplicates, comparators);
    }


    // LEFT JOINS
    public RealtimeJoinBolt leftJoin(String stream, int retentionCount, boolean dropOlderDuplicates, JoinComparator... comparators) {
        return joinHelperCountRetention(JoinType.LEFT, stream, retentionCount, dropOlderDuplicates, comparators);
    }

    public RealtimeJoinBolt leftJoin(String stream, Duration retentionTime, boolean dropOlderDuplicates, JoinComparator... comparators) {
        return joinHelperTimeRetention(JoinType.LEFT, stream, retentionTime, dropOlderDuplicates, comparators);
    }


    // RIGHT JOINS
    public RealtimeJoinBolt rightJoin(String stream, int retentionCount, boolean dropOlderDuplicates, JoinComparator... comparators) {
        return joinHelperCountRetention(JoinType.RIGHT, stream, retentionCount, dropOlderDuplicates, comparators);
    }

    public RealtimeJoinBolt rightJoin(String stream, Duration retentionTime, boolean dropOlderDuplicates, JoinComparator... comparators) {
        return joinHelperTimeRetention(JoinType.RIGHT, stream, retentionTime, dropOlderDuplicates, comparators);
    }


    // OUTER JOINS
    public RealtimeJoinBolt outerJoin(String stream, int retentionCount, boolean dropOlderDuplicates, JoinComparator... comparators) {
        return joinHelperCountRetention(JoinType.OUTER, stream, retentionCount, dropOlderDuplicates, comparators);
    }

    public RealtimeJoinBolt outerJoin(String stream, Duration retentionTime, boolean dropOlderDuplicates, JoinComparator... comparators) {
        return joinHelperTimeRetention(JoinType.OUTER, stream, retentionTime, dropOlderDuplicates, comparators);
    }


    private RealtimeJoinBolt joinHelperCountRetention(JoinType joinType, String stream, int retentionCount, boolean dropOlderDuplicates, JoinComparator[] comparators) {
        // 1- Check stream names and make explicit any implicit stream names
        validateAndSetupStreamNames(comparators, stream);

        // 2- Check and set up the field names
        if (this.streamCount==-1)
            throw  new IllegalArgumentException("Must call from() before calling any of the *join() methods");
        if (retentionCount<=0)
            throw  new IllegalArgumentException("Retention count must be positive number");

        ++streamCount;
        this.joinInfos.put(stream, new JoinInfo(joinType, null, retentionCount, dropOlderDuplicates, comparators) );

        return this;
    }

    private RealtimeJoinBolt joinHelperTimeRetention(JoinType joinType, String stream, Duration retentionTime, boolean dropOlderDuplicates, JoinComparator[] comparators) {
        // 1- Check stream names and make explicit any implicit stream names
        validateAndSetupStreamNames(comparators, stream);

        // 2- Check and set up the field names
        if (this.streamCount==-1)
            throw  new IllegalArgumentException("Must call from() before calling any of the *join() methods");
        if (retentionTime.toMillis()<=0)
            throw  new IllegalArgumentException("Retention count must be positive number");

        ++streamCount;
        this.joinInfos.put(stream, new JoinInfo(joinType, retentionTime.toMillis(), null, dropOlderDuplicates, comparators) );

        return this;
    }

    private void validateAndSetupStreamNames(JoinComparator[] comparators, String currentStream) {
        for (JoinComparator c : comparators) {
            FieldSelector f1 = c.getFieldSelector1();
            FieldSelector f2 = c.getFieldSelector2();

            String s1 = f1.streamName;
            String s2 = f2.streamName;
            if (s1 == null && s2==null)
                throw  new InvalidArgumentException("Either one or both field selectors must have a explicity stream qualifier in a join condition: '"
                        + f1.canonicalFieldName() + "' & '" + f2.canonicalFieldName() + "'");
            if (s1!=null && s2!=null && s1.equalsIgnoreCase(s2))
                throw  new InvalidArgumentException("Both field selectors must cannot have same stream prefix: '" + f1.outputName + "' & '" + f2.outputName + "'");


            if ( f1.streamName == null) {
                f1.streamName = currentStream; // make it explicit
            } else  if ( f2.streamName == null) {
                f2.streamName = currentStream;
            }
        }
    }


    /**
     * Specify output fields
     *      e.g: .select("lookupField, stream2:dataField, field3")
     * Nested Key names are supported for nested types:
     *      e.g: .select("outerKey1.innerKey1, outerKey1.innerKey2, stream3:outerKey2.innerKey3)"
     * Inner types (non leaf) must be Map<> in order to support nested lookup using this dot notation
     * This selected fields implicitly declare the output fieldNames for the bolt based.
     * @param commaSeparatedKeys
     * @return
     */
    public RealtimeJoinBolt select(String commaSeparatedKeys) {
        String[] fieldNames = commaSeparatedKeys.split(",");

        outputFields = new FieldSelector[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            outputFields[i] = new FieldSelector(fieldNames[i], streamKind);
        }
        return this;
    }

    /** Convenience method for Streamline that prefixes each keyname with 'streamline-event.'
     *
     * @param commaSeparatedKeys
     * @return
     */
    public RealtimeJoinBolt streamlineSelect(String commaSeparatedKeys) {
        String prefixedKeys = convertToStreamLineKeys(commaSeparatedKeys);
        streamLineProjection = true;
        return  select(prefixedKeys);
    }

    public RealtimeJoinBolt withOutputStream(String streamName) {
        this.outputStream = streamName;
        return this;
    }

    @VisibleForTesting
    public String[] getOutputFields() {
        String[] result = new String[outputFields.length];
        for (int i = 0; i < outputFields.length; i++) {
            result[i] = outputFields[i].outputName;
        }
        return result;
    }

    @Override
    public void execute(Tuple tuple) {
        if (timeBasedRetention) {
            expireAndAckTimedOutEntries(lookupBuffer);
        }

        try {
            String streamId = streamKind.getStreamId(tuple);
            if ( isLookupStream(streamId) ) {
                processLookupStreamTuple(tuple);
            } else if (isDataStream(streamId) ) {
                processDataStreamTuple(tuple);
            } else {
                throw new InvalidTuple("Received tuple from unexpected stream/source : " + streamId, tuple);
            }
        } catch (InvalidTuple e) {
            collector.ack(tuple);
            LOG.warn("{}. Tuple will be dropped.",  e.toString());
        }
    }

//    private void processDataStreamTuple(Tuple tuple) throws InvalidTuple {
//        List<TupleInfo> matches = matchWithLookupStream(tuple);
//        if (matches==null || matches.isEmpty() ) {  // no match
//            if (joinType== JoinType.LEFT ||  joinType== JoinType.OUTER ) {
//                List<Object> outputTuple = doProjection(tuple, null);
//                emit(outputTuple, tuple);
//                return;
//            }
//            collector.ack(tuple);
//        }  else {
//            for (TupleInfo lookupTuple : matches) { // match found
//                lookupTuple.unmatched = false;
//                List<Object> outputTuple = doProjection(lookupTuple.tuple, tuple);
//                emit(outputTuple, tuple, lookupTuple.tuple);
//            }
//            collector.ack(tuple);
//        }
//    }
//
//    private void processLookupStreamTuple(Tuple tuple) throws InvalidTuple {
//        String key = makeLookupTupleKey(tuple);
//        if (dropOlderDuplicates)
//            lookupBuffer.removeAll(key);
//        lookupBuffer.put(key, new TupleInfo(tuple) );
//
//        if (timeBasedRetention) {
//            timeTracker.add(System.currentTimeMillis());
//        } else {  // count based Rotation
//            if (lookupBuffer.size() > retentionCount) {
//                TupleInfo expired = removeHead(lookupBuffer);
//                if( (joinType== JoinType.RIGHT) || (joinType== JoinType.OUTER)  ) {
//                    emitUnMatchedTuples(expired);
//                }
//                collector.ack(expired.tuple);
//            }
//        }
//    }

    private void processTuple(Tuple tuple) throws InvalidTuple {
        String stream = streamKind.getStreamId(tuple);
        Integer streamIndex = streamIndexes.get(stream);
        if (streamIndex==null) {
            throw new InvalidTuple("Source component/streamId for Tuple not part of streams being joined : " + stream, tuple);
        }

        // 1- do join
        List<TupleInfo> matches = matchWithLookupStream(tuple);
        if (matches==null || matches.isEmpty() ) {  // no match
            if (joinType.get(streamIndex)==JoinType.LEFT ||  joinType.get(streamIndex)==JoinType.OUTER ) {
                List<Object> outputTuple = doProjection(tuple, null);
                emit(outputTuple, tuple);
                return;
            }
            collector.ack(tuple);
        }  else {
            for (TupleInfo lookupTuple : matches) { // match found
                lookupTuple.unmatched = false;
                List<Object> outputTuple = doProjection(lookupTuple.tuple, tuple);
                emit(outputTuple, tuple, lookupTuple.tuple);
            }
            collector.ack(tuple);
        }

        // 2- buffer

        String key = getKey(tuple, streamIndex);
        if (dropOlderDuplicates.get(streamIndex))
            streamBuffers[streamIndex].removeAll(key);
        streamBuffers[streamIndex].put(key, new TupleInfo(tuple) );

        if(timeBasedRetention) {
            timeTracker.add(System.currentTimeMillis());
        } else {  // count based Rotation
            if (lookupBuffer.size() > retentionCount) {
                TupleInfo expired = removeHead(lookupBuffer);
                if( (joinType==JoinType.RIGHT) || (joinType==JoinType.OUTER)  ) {
                    emitUnMatchedTuples(expired);
                }
                collector.ack(expired.tuple);
            }
        }
    }


    private String getKey(Tuple tuple, Integer streamIndex) throws InvalidTuple {
        StringBuilder key = new StringBuilder();
        for (JoinInfo ji : joinCriteria) {
            Object partialKey = ji.fields[streamIndex].findField(tuple);
            if (partialKey==null)
                throw new InvalidTuple("'" +ji.fields[streamIndex] + "' field is missing in the tuple", tuple);
            key.append( partialKey.toString() );
            key.append(".");
        }
        return key.toString();
    }

    private String makeLookupTupleKey(Tuple tuple) throws InvalidTuple {
        StringBuilder key = new StringBuilder();
        for (JoinInfo ji : joinCriteria) {
            Object partialKey = ji.fields[0].findField(tuple);
            if (partialKey==null)
                throw new InvalidTuple("'" +ji.fields[0] + "' field is missing in the tuple", tuple);
            key.append( partialKey.toString() );
            key.append(".");
        }
        return key.toString();
    }

    private String makeDataTupleKey(Tuple tuple)  throws InvalidTuple  {
        StringBuilder key = new StringBuilder();
        for (JoinInfo ji : joinCriteria) {
            Object partialKey = ji.fields[1].findField(tuple);
            if (partialKey==null)
                throw new InvalidTuple("'" + ji.fields[1] + " field is is missing in the tuple", tuple);
            key.append( partialKey.toString() );
            key.append(".");
        }
        return key.toString();
    }

    // returns null if no match
    private List<TupleInfo> matchWithLookupStream(Tuple lookupTuple) throws InvalidTuple {
        String key = makeDataTupleKey(lookupTuple);
        return lookupBuffer.get(key);
    }

    // Removes timedout entries from lookupBuffer & timeTracker. Acks tuples being expired.
    private void expireAndAckTimedOutEntries(LinkedListMultimap<String, TupleInfo> lookupBuffer) {
        Long expirationTime = System.currentTimeMillis() - retentionTime;
        Long  insertionTime = timeTracker.peek();
        while ( insertionTime!=null  &&   expirationTime > insertionTime ) {
            TupleInfo expired = removeHead(lookupBuffer);
            timeTracker.pop();
            if ( joinType == JoinType.RIGHT || joinType == JoinType.OUTER )
                emitUnMatchedTuples(expired);
            collector.ack(expired.tuple);
            insertionTime = timeTracker.peek();
        }
    }

    private void emitUnMatchedTuples(TupleInfo expired) {
        if(expired.unmatched) {
            List<Object> outputTuple = doProjection(expired.tuple, null);
            emit(outputTuple, expired.tuple);
        }
    }

    private static TupleInfo removeHead(LinkedListMultimap<String, TupleInfo> lookupBuffer) {
        List<Map.Entry<String, TupleInfo>> entries = lookupBuffer.entries();
        return entries.remove(0).getValue();
    }


    private void emit(List<Object> outputTuple, Tuple anchor) {
        if ( outputStream ==null )
            collector.emit(anchor, outputTuple);
        else
            collector.emit(outputStream, anchor, outputTuple);
    }

    private void emit(List<Object> outputTuple, Tuple dataTupleAnchor, Tuple lookupTupleAnchor) {
        List<Tuple> anchors = Arrays.asList(dataTupleAnchor, lookupTupleAnchor);
        if ( outputStream ==null )
            collector.emit(anchors, outputTuple);
        else
            collector.emit(outputStream, anchors, outputTuple);
    }

    private boolean isDataStream(String streamId) {
        return streamId.equals(dataStream);
    }

    private boolean isLookupStream(String streamId) {
        return streamId.equals(lookupStream);
    }

    /** Performs projection on the tuples based on 'projectionFields'
     *
     * @param tuple1   can be null
     * @param tuple2   can be null
     * @return   project fields
     */
    protected List<Object> doProjection(Tuple tuple1, Tuple tuple2) {
        if(streamLineProjection)
            return doStreamlineProjection(tuple1, tuple2);

        ArrayList<Object> result = new ArrayList<>(outputFields.length);
        for ( int i = 0; i < outputFields.length; i++ ) {
            FieldSelector outField = outputFields[i];
            Object field = outField.findField(tuple1) ;
            if (field==null)
                field = outField.findField(tuple2);
            result.add(field); // adds null if field is not found in both tuples
        }
        return result;
    }

    // NOTE: Streamline specific convenience method. Creates output tuple as a StreamlineEvent
    protected List<Object> doStreamlineProjection(Tuple tuple1, Tuple tuple2) {
//        String flattenedKey = projectionKeys[i].getOutputName();
//        String outputKeyName = dropStreamLineEventPrefix(flattenedKey); // drops the "streamline-event." prefix
        StreamlineEventImpl.Builder eventBuilder = StreamlineEventImpl.builder();

        for ( int i = 0; i < outputFields.length; i++ ) {
            FieldSelector outField = outputFields[i];

            Object field = outField.findField(tuple1) ;
            if (field==null)
                field = outField.findField(tuple2);
            String outputKeyName = dropStreamLineEventPrefix(outField.outputName );
            eventBuilder.put(outputKeyName, field); // adds null if field is not found in both tuples
        }

        StreamlineEventImpl slEvent = eventBuilder.dataSourceId("multiple sources").build();
        return Collections.singletonList(slEvent);
    }


    // Prefixes each key with 'streamline-event.' Example:
    //   arg = "stream1:key1, key2, stream2:key3.key4, key5"
    //   result  = "stream1:streamline-event.key1, streamline-event.key2, stream2:streamline-event.key3.key4, streamline-event.key5"
    private String convertToStreamLineKeys(String commaSeparatedKeys) {
        String[] keyNames = commaSeparatedKeys.replaceAll("\\s+","").split(",");

        String[] prefixedKeys = new String[keyNames.length];

        for (int i = 0; i < keyNames.length; i++) {
            FieldSelector fs = new FieldSelector(keyNames[i], streamKind);
            if (fs.streamName==null)
                prefixedKeys[i] =  EVENT_PREFIX +  fs.canonicalFieldName();
            else
                prefixedKeys[i] =  fs.streamName + ":" + EVENT_PREFIX + fs.canonicalFieldName();
        }

        return String.join(", ", prefixedKeys);
    }

    private static String dropStreamLineEventPrefix(String flattenedKey) {
        int pos = flattenedKey.indexOf(EVENT_PREFIX);
        if(pos==0)
            return flattenedKey.substring(EVENT_PREFIX.length());
        return flattenedKey.substring(0,pos) + flattenedKey.substring(pos+EVENT_PREFIX.length());
    }
}


class FieldSelector implements Serializable {
    final static long serialVersionUID = 2L;
    final static Pattern fieldDescrPattern = Pattern.compile("(?:([\\w-]+?):)?([\\w.-]+)(?: +as +([\\w.-]+))? *");
    final RealtimeJoinBolt.StreamKind streamKind;

    String streamName;     // can be null;. StreamKind name can have '-' & '_'
    String[] field;        // nested field "x.y.z"  becomes => String["x","y","z"]. Field names can contain '-' & '_'
    private String alias;  // can be null. In 'strm:x.y.z as z', here z is the alias (alias can contain '-', '_' &'.')
    String outputName;     // either "stream1:x.y.z" or "x.y.z" (if stream unspecified) or just alias.

    public FieldSelector(String fieldDescriptor, RealtimeJoinBolt.StreamKind streamKind)  {
        this.streamKind = streamKind;

        int pos = fieldDescriptor.indexOf(':');
        Matcher matcher = fieldDescrPattern.matcher(fieldDescriptor);
        if (!matcher.find( ))
            throw new IllegalArgumentException("'" +fieldDescriptor + "' is not a valid field descriptor. Correct Format: [streamid:]nested.field [as anAlias]");
        this.streamName = matcher.group(1);     // can be null
        String fieldDesc = matcher.group(2);
        if (fieldDesc==null)
            throw new IllegalArgumentException("'" +fieldDescriptor + "' is not a valid field descriptor. Correct Format: [streamid:]nested.field [as anAlias]");
        this.field = fieldDesc.split("\\.");
        this.alias = matcher.group(3);   // can be bykk

        if (alias!=null)
            outputName = alias;
        else
            outputName = (streamName==null) ? fieldDesc :  streamName+":"+fieldDesc ;
    }

    /**
     * @param stream name of stream
     * @param fieldDescriptor  Simple fieldDescriptor like "x.y.z" and without a stream qualifier prefix 'stream1:'.
     */
    public FieldSelector(String stream, String fieldDescriptor, RealtimeJoinBolt.StreamKind streamKind)  {
        this(stream + ":" + fieldDescriptor, streamKind);
        if(fieldDescriptor.indexOf(":")>=0) {
            throw new IllegalArgumentException("Not expecting stream qualifier ':' in '" + fieldDescriptor
                    + "'. Stream '" + stream +  "' is separately provided in this context");
        }
        this.streamName = stream;
    }


    // returns field name in x.y.z format (without stream name)
    public String canonicalFieldName() {
        return String.join(".", field);
    }


    @Override
    public String toString() {
        return outputName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        try {
            FieldSelector that = (FieldSelector) o;
            return outputName != null ? outputName.equals(that.outputName) : that.outputName == null;
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return outputName != null ? outputName.hashCode() : 0;
    }

    // Extract the field from tuple. Can be a nested field (x.y.z)
    // returns null if not found
    public Object findField(Tuple tuple) {
        if (tuple==null) {
            return null;
        }
        // verify stream name matches, if stream name was specified
        if ( streamName!=null &&
                !streamName.equalsIgnoreCase( streamKind.getStreamId(tuple) ) ) {
            return null;
        }

        Object curr = null;
        for (int i=0; i < field.length; i++) {
            if (i==0) {
                if (tuple.contains(field[i]) )
                    curr = tuple.getValueByField(field[i]);
                else
                    return null;
            }  else  {
                curr = ((Map) curr).get(field[i]);
                if (curr==null)
                    return null;
            }
        }
        return curr;
    }

} // class FieldSelector

class JoinInfo implements Serializable {
    final static long serialVersionUID = 1L;

    final JoinType joinType;              // null for first stream defined via from()
    final Long retentionTime;             // in millis. can be null.
    final Integer retentionCount;         // can be null
    final Boolean dropOlderDuplicates;
    final JoinComparator[] comparators;   // null for first stream defined via from()

    final ArrayDeque<Long> timeTracker;   // for time based retention. tracks time at which the tuples were received
    final ArrayDeque<Tuple> buffer;       // retention window

    public JoinInfo(JoinType joinType, Long retentionTimeMs, Integer retentionCount, Boolean dropOlderDuplicates, JoinComparator... comparators) {
        this.joinType = joinType;
        this.retentionTime = retentionTimeMs;
        this.retentionCount = retentionCount;
        this.dropOlderDuplicates = dropOlderDuplicates;
        this.comparators = comparators;
        int estimateWindowSz = retentionCount != null ? retentionCount : 100_000;
        this.timeTracker = new ArrayDeque<Long>( estimateWindowSz );
        this.buffer = new ArrayDeque<Tuple>( estimateWindowSz );
    }
} // class JoinInfo
