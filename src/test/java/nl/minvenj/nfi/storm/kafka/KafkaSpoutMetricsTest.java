package nl.minvenj.nfi.storm.kafka;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;
import nl.minvenj.nfi.storm.kafka.util.metric.KafkaOffsetMetric;
import nl.minvenj.nfi.storm.kafka.util.metric.MultiAssignableMetric;

public class KafkaSpoutMetricsTest {
    private KafkaSpout _subject;

    @Before
    public void setup() {
        _subject = new KafkaSpout();
        _subject._topic = "test-topic";
        _subject._bufSize = 4;

        // add three queued messages (4, 6 and 8 bytes long)
        final KafkaMessageId id1 = new KafkaMessageId(1, 1234);
        _subject._queue.add(id1);
        _subject._inProgress.put(id1, new byte[]{1, 2, 3, 4});
        final KafkaMessageId id2 = new KafkaMessageId(1, 123);
        _subject._queue.add(id2);
        _subject._inProgress.put(id2, new byte[]{1, 2, 3, 4, 5, 6});
        final KafkaMessageId id3 = new KafkaMessageId(2, 12345);
        _subject._queue.add(id3);
        _subject._inProgress.put(id3, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        // make sure a collector is present
        _subject._collector = mock(SpoutOutputCollector.class);

        final Map<String, List<KafkaStream<byte[], byte[]>>> stream = new HashMap<String, List<KafkaStream<byte[], byte[]>>>() {{
            final KafkaStream<byte[], byte[]> mockedStream = mock(KafkaStream.class);
            final ConsumerIterator<byte[], byte[]> iterator = mock(ConsumerIterator.class);
            // make the iterator indicate a next message available once
            when(iterator.hasNext()).thenReturn(true);
            when(iterator.next()).thenReturn(new MessageAndMetadata<byte[], byte[]>(
                new byte[]{},
                new byte[]{},
                "test-topic",
                1,
                1234
            )).thenThrow(ConsumerTimeoutException.class);
            when(mockedStream.iterator()).thenReturn(iterator);
            put("test-topic", Arrays.asList(mockedStream));
        }};
        _subject._consumer = mock(ConsumerConnector.class);
        when(_subject._consumer.createMessageStreams(any(Map.class))).thenReturn(stream);
    }

    @Test
    public void testBufferLoadMetric() {
        _subject._bufferStateMetric = new MultiAssignableMetric<Number>();
        _subject._queue.clear();
        _subject._inProgress.clear();

        final Object originalLoad = _subject._bufferStateMetric.getValueAndReset().get(KafkaSpout.METRIC_BUFFER_LOAD);
        assertNull(originalLoad);
        // stream contains a single message, buffer size is 4, load should be 0.25
        _subject.fillBuffer();
        assertEquals(0.25, (Double) _subject._bufferStateMetric.getValueAndReset().get(KafkaSpout.METRIC_BUFFER_LOAD), 0.01);

        _subject._inProgress.clear();
        _subject._queue.clear();
        // refill buffer, without messages in the stream should yield a load of 0.0
        _subject.fillBuffer();
        assertEquals(0.0, (Double) _subject._bufferStateMetric.getValueAndReset().get(KafkaSpout.METRIC_BUFFER_LOAD), 0.01);
    }

    @Test
    public void testFillTimeMetric() {
        _subject._bufferStateMetric = new MultiAssignableMetric<Number>();
        _subject._queue.clear();
        _subject._inProgress.clear();

        _subject.fillBuffer();
        final long fillTime = _subject._bufferStateMetric.getValueAndReset().get(KafkaSpout.METRIC_BUFFER_FILL_TIME).longValue();
        // no delays, fill time should be ~0
        assertThat(fillTime, greaterThanOrEqualTo(0L));
        assertThat(fillTime, lessThanOrEqualTo(20L));
    }

    @Test
    public void testFillIntervalMetric() {
        _subject._bufferStateMetric = new MultiAssignableMetric<Number>();
        _subject._queue.clear();
        _subject._inProgress.clear();

        // assert start state
        assertEquals(0L, _subject._lastFillTime);
        _subject.fillBuffer();
        // assert no non-sensical interval has been set
        assertNull(_subject._bufferStateMetric.getValueAndReset().get(KafkaSpout.METRIC_BUFFER_FILL_INTERVAL));
        // assert last fill time has been set
        assertThat(_subject._lastFillTime, greaterThan(0L));
        assertThat(_subject._lastFillTime, lessThanOrEqualTo(System.currentTimeMillis()));

        // clear and fill again
        _subject._queue.clear();
        _subject._inProgress.clear();
        _subject.fillBuffer();
        // assert metric was set
        assertNotNull(_subject._bufferStateMetric.getValueAndReset().get(KafkaSpout.METRIC_BUFFER_FILL_INTERVAL));
        final long fillInterval = _subject._bufferStateMetric.getValueAndReset().get(KafkaSpout.METRIC_BUFFER_FILL_INTERVAL).longValue();
        // no delays, fill interval should be ~0
        assertThat(fillInterval, greaterThanOrEqualTo(0L));
        assertThat(fillInterval, lessThanOrEqualTo(20L));
    }

    @Test
    public void testEmittedBytesMetric() {
        _subject._emittedBytesMetric = new CountMetric();
        final long originalTotal = (Long) _subject._emittedBytesMetric.getValueAndReset();
        assertEquals(originalTotal, 0);

        // emit a single 4-byte message queued and verify the counter has been incremented
        _subject.nextTuple();
        assertEquals(4, ((Long) _subject._emittedBytesMetric.getValueAndReset()).longValue());

        // emit the rest of the messages and verify the counter acts as a sum
        _subject.nextTuple();
        _subject.nextTuple();
        assertEquals(14, ((Long) _subject._emittedBytesMetric.getValueAndReset()).longValue());
    }

    @Test
    public void testKafkaOffsetMetric() {
        _subject._emittedOffsetsMetric = new KafkaOffsetMetric();

        // emit all queued messages
        _subject.nextTuple();
        _subject.nextTuple();
        _subject.nextTuple();

        final Map<Integer, Long> value = _subject._emittedOffsetsMetric.getValueAndReset();
        // verify two partitions
        assertEquals(2, value.size());
        assertEquals(1234L, (long) value.get(1));
        assertEquals(12345L, (long) value.get(2));
    }
}
