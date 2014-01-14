/**
 * Copyright 2013 Netherlands Forensic Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.minvenj.nfi.storm.kafka;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import nl.minvenj.nfi.storm.kafka.util.ConfigUtils;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;

/**
 * Tests the behaviour of the kafka spout with regards to it's buffer and acknowledgements / failures.
 *
 * NB: default buffering behaviour operates on default failure policy (reliable).
 *
 * @see KafkaSpoutFailurePolicyTest
 */
public class KafkaSpoutBufferBehaviourTest {
    // stream mapping of a single stream that won't provide messages
    protected static final Map<String, List<KafkaStream<byte[], byte[]>>> EMPTY_STREAM = new HashMap<String, List<KafkaStream<byte[], byte[]>>>() {{
        final KafkaStream<byte[], byte[]> mockedStream = mock(KafkaStream.class);
        when(mockedStream.iterator()).thenReturn(mock(ConsumerIterator.class));
        put("test-topic", Arrays.asList(mockedStream));
    }};
    private KafkaSpout _subject;
    private ConsumerConnector _consumer;
    private Map<String, List<KafkaStream<byte[], byte[]>>> _stream;

    @Before
    public void setup() {
        // main test subject
        _subject = new KafkaSpout();

        // assign the topic to be used for stream retrieval
        _subject._topic = "test-topic";
        // use a buffer size higher than the expected amount of messages available
        _subject._bufSize = 4;
        // assign the default FailHandler
        _subject._failHandler = ConfigUtils.DEFAULT_FAIL_HANDLER;
        // mocked consumer to avoid actually contacting zookeeper
        _consumer = mock(ConsumerConnector.class);
        // ... but make sure it will return a valid (empty) stream
        when(_consumer.createMessageStreams(any(Map.class))).thenReturn(EMPTY_STREAM);
        // assign the consumer to the test subject
        _subject._consumer = _consumer;
        // provide a mocked collector to be able to check for emitted values
        _subject._collector = mock(SpoutOutputCollector.class);

        // create a new single message stream for every test
        _stream = new HashMap<String, List<KafkaStream<byte[], byte[]>>>() {{
            final KafkaStream<byte[], byte[]> mockedStream = mock(KafkaStream.class);
            final ConsumerIterator<byte[], byte[]> iterator = mock(ConsumerIterator.class);
            // make the iterator indicate a next message available once
            when(iterator.hasNext()).thenReturn(true);
            when(iterator.next()).thenReturn(new MessageAndMetadata<byte[], byte[]>(
                    new byte[0],
                    new byte[0],
                    "test-topic",
                    1,
                    1234
            )).thenThrow(ConsumerTimeoutException.class);
            when(mockedStream.iterator()).thenReturn(iterator);
            put("test-topic", Arrays.asList(mockedStream));
        }};
    }

    @Test
    public void testDeclarations() {
        final OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

        _subject.declareOutputFields(declarer);
        // verify the spout declares to output single-field tuples
        verify(declarer).declare(argThat(new ArgumentMatcher<Fields>() {
            @Override
            public boolean matches(final Object argument) {
                final Fields fields = (Fields) argument;
                return fields.size() == 1 && fields.get(0).equals("bytes");
            }
        }));

        // verify the spout will not provide component configuration
        assertNull(_subject.getComponentConfiguration());
    }

    @Test
    public void testInitiallyEmpty() {
        assertTrue(_subject._queue.isEmpty());
        assertTrue(_subject._inProgress.isEmpty());
    }

    @Test
    public void testRefillOnEmpty() {
        // request activity from subject
        _subject.nextTuple();

        // verify that subject requested more messages from the kafka consumer
        verify(_consumer).createMessageStreams(any(Map.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testRefuseRefillOnNonEmptyBuffer() {
        _subject._queue.add(new KafkaMessageId(1, 1234));
        _subject.fillBuffer();
    }

    @Test(expected = IllegalStateException.class)
    public void testRefuseRefillOnNonEmptyPending() {
        _subject._inProgress.put(new KafkaMessageId(1, 1234), new byte[0]);
        _subject.fillBuffer();
    }

    @Test(expected = IllegalStateException.class)
    public void testRefuseRefillOnNonEmptyBoth() {
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        _subject._queue.add(id);
        _subject._inProgress.put(id, new byte[0]);
        _subject.fillBuffer();
    }

    @Test
    public void testRefillBothOnMessageAvailable() {
        // NB: update the consumer mock for this test to return the single message stream
        when(_consumer.createMessageStreams(any(Map.class))).thenReturn(_stream);

        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        // test whether the single message in the stream is added to both the buffer and pending
        _subject.fillBuffer();
        assertEquals(1, _subject._queue.size());
        assertEquals(id, _subject._queue.peek());
        assertEquals(1, _subject._inProgress.size());
        assertEquals(id, _subject._inProgress.firstKey());
    }

    @Test
    public void testBufferLoadMetric() {
        // NB: update the consumer mock for this test to return the single message stream
        when(_consumer.createMessageStreams(any(Map.class))).thenReturn(_stream);
        _subject._bufferLoadMetric = new AssignableMetric(0.0);

        final double originalLoad = (Double) _subject._bufferLoadMetric.getValueAndReset();
        assertEquals(originalLoad, 0.0, 0.01);
        // stream contains a single message, buffer size is 4, load should be 0.25
        _subject.fillBuffer();
        assertEquals(0.25, (Double) _subject._bufferLoadMetric.getValueAndReset(), 0.01);

        _subject._inProgress.clear();
        _subject._queue.clear();
        // refill buffer, without messages in the stream should yield a load of 0.0
        _subject.fillBuffer();
        assertEquals(0.0, (Double) _subject._bufferLoadMetric.getValueAndReset(), 0.01);
    }

    @Test
    public void testEmitOnAvailable() {
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        final byte[] message = {5, 6, 7, 8};
        _subject._queue.add(id);
        _subject._inProgress.put(id, message);

        // request to emit message and id
        _subject.nextTuple();

        // subject should have emitted a Values object identified by id
        verify(_subject._collector).emit(eq(new Values(message)), eq(id));
    }

    @Test
    public void testEmitOneAtATime() {
        final KafkaMessageId id1 = new KafkaMessageId(1, 1234);
        final KafkaMessageId id2 = new KafkaMessageId(1, 1235);
        final byte[] message1 = {5, 6, 7, 8};
        final byte[] message2 = {9, 0, 1, 2};
        _subject._queue.add(id1);
        _subject._queue.add(id2);
        _subject._inProgress.put(id1, message1);
        _subject._inProgress.put(id2, message2);

        _subject.nextTuple();

        // subject should have emitted only the first message
        verify(_subject._collector).emit(eq(new Values(message1)), eq(id1));
        verifyNoMoreInteractions(_subject._collector);
    }

    @Test
    public void testIllegalQueueState() {
        // queue a single id with no corresponding message
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        _subject._queue.add(id);

        try {
            _subject.nextTuple();
            fail("illegal queue state didn't trigger error");
        }
        catch (final IllegalStateException e) {
            assertThat(e.getMessage(), containsString(id.toString()));
        }
    }

    @Test
    public void testEmittedBytesMetric() {
        // NB: update the consumer mock for this test to return the single message stream
        when(_consumer.createMessageStreams(any(Map.class))).thenReturn(_stream);
        _subject._emittedBytesMetric = new CountMetric();

        final long originalTotal = (Long) _subject._emittedBytesMetric.getValueAndReset();
        assertEquals(originalTotal, 0);

        // emit the single 4-byte message in the stream and verify the counter has been incremented
        _subject.nextTuple();
        assertEquals(4, ((Long) _subject._emittedBytesMetric.getValueAndReset()).longValue());
    }

    @Test
    public void testAck() {
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        _subject._queue.add(id);
        _subject._inProgress.put(id, new byte[0]);

        _subject.nextTuple();
        // verify that the message left buffer but not pending
        assertTrue(_subject._queue.isEmpty());
        assertTrue(_subject._inProgress.containsKey(id));

        _subject.ack(id);
        // verify that the buffer is still empty and the key is no longer in pending
        assertTrue(_subject._queue.isEmpty());
        assertFalse(_subject._inProgress.containsKey(id));

        // verify that a non-KafkaMessageId argument is ignored
        final SortedMap<KafkaMessageId, byte[]> spy = spy(_subject._inProgress);
        _subject.ack(new Object());
        verifyNoMoreInteractions(spy);
    }

    @Test
    public void testFail() {
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        _subject._queue.add(id);
        _subject._inProgress.put(id, new byte[0]);

        _subject.nextTuple();
        // verify that the message left buffer but not pending
        assertTrue(_subject._queue.isEmpty());
        assertTrue(_subject._inProgress.containsKey(id));

        _subject.fail(id);
        // verify that the buffer is no longer empty and id is still pending
        assertFalse(_subject._queue.isEmpty());
        assertTrue(_subject._inProgress.containsKey(id));

        _subject.nextTuple();
        // verify that the buffer is once again empty and the id has been emitted twice
        assertTrue(_subject._queue.isEmpty());
        verify(_subject._collector, times(2)).emit(any(Values.class), eq(id));

        // verify that a non-KafkaMessageId argument is ignored
        final SortedMap<KafkaMessageId, byte[]> spy = spy(_subject._inProgress);
        _subject.fail(new Object());
        verifyNoMoreInteractions(spy);
    }
}
