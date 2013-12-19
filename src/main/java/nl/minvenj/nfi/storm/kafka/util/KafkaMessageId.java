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

package nl.minvenj.nfi.storm.kafka.util;

import java.io.Serializable;

/**
 * Convenience class representing an orderable 2-tuple of a kafka message's partition and offset within that partition.
 * The natural order of {@link KafkaMessageId} is identical to the ordering in a kafka partition.
 *
 * @author Netherlands Forensics Institute
 */
public class KafkaMessageId implements Comparable<KafkaMessageId>, Serializable {
    private final int _partition;
    private final long _offset;

    public KafkaMessageId(final int partition, final long offset) {
        _partition = partition;
        _offset = offset;
    }

    public int getPartition() {
        return _partition;
    }

    public long getOffset() {
        return _offset;
    }

    /**
     * {@link KafkaMessageId}s are considered equal when both their partition and offset are identical.
     *
     * @param o The object to compare with.
     * @return Whether {@code o} is considered to be equal to this {@link KafkaMessageId}.
     */
    @Override
    public boolean equals(final Object o) {
        if (o instanceof KafkaMessageId) {
            final KafkaMessageId other = (KafkaMessageId) o;
            return other.getPartition() == _partition && other.getOffset() == _offset;
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // create a hash code using all bits of both identifying members
        return (31 + _partition) * (int) (_offset ^ (_offset >>> 32));
    }

    /**
     * Compares this {@link KafkaMessageId} to {@code id}. Comparison is made numerically, where the partition is
     * considered more significant than the offset within the partition. The resulting ordering of
     * {@link KafkaMessageId} is identical to the ordering in a kafka partition.
     * An instance is considered greater than {@code null}.
     *
     * @param id The {@link KafkaMessageId} to compare with.
     * @return The result of {@code 2 * signum(partition - id.getPartition()) + signum(offset - id.getOffset())} or
     *         {@code 1} if {@code id} is null.
     */
    @Override
    public int compareTo(final KafkaMessageId id) {
        // instance is always > null
        if (id == null) {
            return 1;
        }
        // use signum to perform the comparison, mark _partition more significant than _offset
        return 2 * Integer.signum(_partition - id.getPartition()) + Long.signum(_offset - id.getOffset());
    }

    @Override
    public String toString() {
        return "(" + _partition + "," + _offset + ")";
    }
}
