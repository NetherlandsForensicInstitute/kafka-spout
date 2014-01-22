package nl.minvenj.nfi.storm.kafka.util.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.metric.api.IMetric;
import nl.minvenj.nfi.storm.kafka.KafkaSpout;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;

/**
 * Metric that tracks the highest emitted offsets per partition for messages emitted from a {@link KafkaSpout}. The
 * value returned by {@link #getValueAndReset()} is a mapping of partitions to the highest emitted offset for that
 * partition.
 *
 * @author Netherlands Forensics Institute
 */
public class KafkaOffsetMetric implements IMetric {
    protected final Map<Integer, Long> _offsets = new ConcurrentHashMap<Integer, Long>();

    public KafkaOffsetMetric() {
    }

    /**
     * Updates the partition mapping with the provided message id if it's offset is greater than the currently stored
     * value for that partition.
     *
     * @param id The emitted message id.
     */
    public void update(final KafkaMessageId id) {
        update(id.getPartition(), id.getOffset());
    }

    /**
     * Updates the partition mapping with the provided partition and offset if the offset is greater than the currently
     * stored value for that partition.
     *
     * @param partition The partition of the emitted message.
     * @param offset    The offset of the emitted message.
     */
    public void update(final int partition, final long offset) {
        if (!_offsets.containsKey(partition) || offset > _offsets.get(partition)) {
            // update when greater, track the max offset
            _offsets.put(partition, offset);
        }
    }

    @Override
    public Map<Integer, Long> getValueAndReset() {
        // provide a new, independent map
        return new HashMap<Integer, Long>(_offsets);
    }
}
