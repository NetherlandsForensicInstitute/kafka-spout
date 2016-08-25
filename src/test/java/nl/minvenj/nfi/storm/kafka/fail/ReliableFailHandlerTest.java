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

package nl.minvenj.nfi.storm.kafka.fail;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Map;

import org.junit.Test;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;

/**
 * Tests the simple provided {@link ReliableFailHandler} and {@link UnreliableFailHandler}.
 */
public class ReliableFailHandlerTest {
    @Test
    public void testSideEffects() {
        final FailHandler subject = new ReliableFailHandler();
        final KafkaMessageId id = new KafkaMessageId(1, 1234);

        // convenience methods should have no effect
        subject.open(mock(Map.class), mock(TopologyContext.class), mock(SpoutOutputCollector.class));
        subject.activate();
        subject.deactivate();
        subject.close();

        // ack should be ignored
        subject.ack(id);
    }

    @Test
    public void testShouldReplay() {
        final FailHandler subject = new ReliableFailHandler();
        final KafkaMessageId id = new KafkaMessageId(1, 1234);

        // reliable handler should *always* tell the spout to replay
        assertTrue(subject.shouldReplay(id));
    }

    @Test(expected = IllegalStateException.class)
    public void testRefuseFail() {
        final FailHandler subject = new ReliableFailHandler();
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        final byte[] message = {1, 2, 3, 4};

        // failing a message to the reliable handler should *always* throw an exception
        subject.fail(id, message);
    }
}
