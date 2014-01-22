package nl.minvenj.nfi.storm.kafka.util.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.Test;

import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;

public class KafkaOffsetMetricTest {
    @Test
    public void testUpdateIdDelegated() {
        final KafkaOffsetMetric subject = spy(new KafkaOffsetMetric());
        final KafkaMessageId id = new KafkaMessageId(1, 1234);

        subject.update(id);
        // verify the correct values are passed
        verify(subject).update(eq(id.getPartition()), eq(id.getOffset()));
    }

    @Test
    public void testMaxValueTracked() {
        final KafkaOffsetMetric subject = new KafkaOffsetMetric();

        // update two distinct partitions
        subject.update(1, 1234);
        subject.update(1, 123);
        subject.update(2, 12345);

        assertEquals(1234, (long) subject._offsets.get(1));
        assertEquals(12345, (long) subject._offsets.get(2));

        // overwrite max offset of partition 1
        subject.update(1, 12345);

        assertEquals(12345, (long) subject._offsets.get(1));
    }

    @Test
    public void testValueIsCopy() {
        final KafkaOffsetMetric subject = new KafkaOffsetMetric();

        // update two distinct partitions
        subject.update(1, 1234);
        subject.update(2, 12345);

        final Map<Integer, Long> value = subject.getValueAndReset();
        // verify value is not the same object as _offsets
        assertNotSame(subject._offsets, value);

        subject.update(3, 123);
        // verify that additions to _offsets do not influence value
        assertNotEquals(subject._offsets.size(), value.size());
        value.put(4, 123L);
        // verify that updates to value do not influence _offsets
        assertNull(subject._offsets.get(4));
    }
}
