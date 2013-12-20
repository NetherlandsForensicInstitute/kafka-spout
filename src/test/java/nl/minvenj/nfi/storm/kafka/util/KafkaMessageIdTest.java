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

import static org.hamcrest.number.OrderingComparison.comparesEqualTo;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

public class KafkaMessageIdTest {
    @Test
    public void testHashCodeEquals() {
        final KafkaMessageId id1 = new KafkaMessageId(1, 1234);
        final KafkaMessageId id2 = new KafkaMessageId(2, 3456);
        final KafkaMessageId id3 = new KafkaMessageId(1, 1234);

        // assert id1 equals id3, but not id2
        assertNotEquals(id1, id2);
        assertNotEquals(id1.hashCode(), id2.hashCode());
        assertEquals(id1, id3);
        assertEquals(id1.hashCode(), id3.hashCode());
    }

    @Test
    public void testComparison() {
        // test a single subject against 8 different and one equal values
        final KafkaMessageId subject = new KafkaMessageId(1, 1234);

        // test comparisons with null
        assertNotEquals(subject, null);
        assertThat(subject, greaterThan((KafkaMessageId) null));

        assertEquals(subject, new KafkaMessageId(1, 1234));
        assertThat(subject, comparesEqualTo(new KafkaMessageId(1, 1234)));

        // use message id < 0 for testing (comparison is numerical)
        assertThat(subject, greaterThan(new KafkaMessageId(-1, 1234)));
        assertThat(subject, greaterThan(new KafkaMessageId(-1, 123)));
        assertThat(subject, greaterThan(new KafkaMessageId(-1, 12345)));

        assertThat(subject, greaterThan(new KafkaMessageId(0, 0)));
        assertThat(subject, greaterThan(new KafkaMessageId(0, 1234)));
        assertThat(subject, greaterThan(new KafkaMessageId(0, 12345)));
        assertThat(subject, greaterThan(new KafkaMessageId(1, 123)));
        // include test for value < min int
        assertThat(subject, greaterThan(new KafkaMessageId(1, -9876543210L)));

        assertThat(subject, lessThan(new KafkaMessageId(2, 0)));
        assertThat(subject, lessThan(new KafkaMessageId(2, 1234)));
        assertThat(subject, lessThan(new KafkaMessageId(2, 12345)));
        assertThat(subject, lessThan(new KafkaMessageId(1, 12345)));
        assertThat(subject, lessThan(new KafkaMessageId(3, 0)));
        assertThat(subject, lessThan(new KafkaMessageId(3, 123)));
        assertThat(subject, lessThan(new KafkaMessageId(3, 1234)));
        assertThat(subject, lessThan(new KafkaMessageId(3, 12345)));
        // include test for value > max int
        assertThat(subject, lessThan(new KafkaMessageId(1, 9876543210L)));
    }

    @Test
    public void testOrdering() {
        // use a list of ids out of order, with a single duplicate (1,1234)
        final List<KafkaMessageId> ids = Arrays.asList(
            new KafkaMessageId(1, -9876543210L),
            new KafkaMessageId(1, 1234),
            new KafkaMessageId(0, 1234),
            new KafkaMessageId(2, 1234),
            new KafkaMessageId(3, 0),
            new KafkaMessageId(3, 1234),
            new KafkaMessageId(3, 12345),
            new KafkaMessageId(1, 123),
            new KafkaMessageId(1, 1234),
            new KafkaMessageId(1, 12345),
            new KafkaMessageId(1, 9876543210L)
        );
        final SortedSet<KafkaMessageId> subject = new TreeSet<KafkaMessageId>(ids);

        // test the behaviour of a sorted set of message ids is as expected
        assertEquals(10, subject.size()); // ids.size() - 1; a single duplicate was inserted
        assertEquals(0, subject.first().getPartition());
        assertEquals(3, subject.last().getPartition());
        assertEquals(3, subject.subSet(new KafkaMessageId(1, 0), new KafkaMessageId(1, 123456)).size());
    }

    @Test
    public void testToStringEquality() {
        final KafkaMessageId id1 = new KafkaMessageId(1, 1234);
        final KafkaMessageId id2 = new KafkaMessageId(1, 1234);

        assertEquals(id1.toString(), id2.toString());
    }
}
