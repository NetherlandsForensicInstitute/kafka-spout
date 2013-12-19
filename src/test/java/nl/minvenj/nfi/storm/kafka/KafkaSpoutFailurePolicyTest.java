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

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Map;

import nl.minvenj.nfi.storm.kafka.util.ConfigUtils;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import kafka.javaapi.consumer.ConsumerConnector;
import nl.minvenj.nfi.storm.kafka.fail.FailHandler;
import nl.minvenj.nfi.storm.kafka.fail.ReliableFailHandler;
import nl.minvenj.nfi.storm.kafka.fail.UnreliableFailHandler;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;

public class KafkaSpoutFailurePolicyTest {
    protected KafkaSpout _subject;

    @Before
    public void setup() {
        _subject = new KafkaSpout();
    }
    @Test
    public void testCreateFailHandlerNull() {
        _subject.createFailHandler(null);
        assertSame(ConfigUtils.DEFAULT_FAIL_HANDLER, _subject._failHandler);
    }

    @Test
    public void testCreateFailHandlerDelegation() {
        _subject.createFailHandler(UnreliableFailHandler.IDENTIFIER);
        assertThat(_subject._failHandler, instanceOf(UnreliableFailHandler.class));
    }

    @Test
    public void testReliableFailure() {
        final FailHandler reliable = spy(new ReliableFailHandler());
        _subject._failHandler = reliable;
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        final byte[] message = {1, 2, 3, 4};
        _subject._inProgress.put(id, message);

        // fail the message
        _subject.fail(id);

        // verify decision delegate to fail handler and replay
        verify(reliable).shouldReplay(eq(id));
        assertEquals(id, _subject._queue.peek());
        assertTrue(_subject._inProgress.containsKey(id));
    }

    @Test
    public void testUnreliableFailure() {
        final FailHandler unreliable = spy(new UnreliableFailHandler());
        _subject._failHandler = unreliable;
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        final byte[] message = {1, 2, 3, 4};
        _subject._inProgress.put(id, message);

        // fail the message
        _subject.fail(id);

        // verify decision and message delegate to fail handler and no replay
        verify(unreliable).shouldReplay(eq(id));
        verify(unreliable).fail(eq(id), eq(message));
        assertTrue(_subject._queue.isEmpty());
        assertFalse(_subject._inProgress.containsKey(id));
    }

    @Test
    public void testDelegatedCalls() {
        final FailHandler unreliable = spy(new UnreliableFailHandler());
        _subject = new KafkaSpout() {
            @Override
            protected void createConsumer(final Map<String, Object> config) {
                // do nothing (would connect to zookeeper otherwise)
            }

            @Override
            protected void createFailHandler(final String failHandler) {
                _failHandler = unreliable;
            }
        };
        _subject._failHandler = unreliable;
        _subject._consumer = mock(ConsumerConnector.class);

        // call all the methods that should trigger policy notification
        _subject.open(mock(Map.class), mock(TopologyContext.class), mock(SpoutOutputCollector.class));
        verify(unreliable).open(any(Map.class), any(TopologyContext.class), any(SpoutOutputCollector.class));
        _subject.activate();
        verify(unreliable).activate();
        _subject.deactivate();
        verify(unreliable).deactivate();
        _subject.close();
        verify(unreliable).close();

        // NB: _subject will have set consumer to null, mock a new one
        _subject._consumer = mock(ConsumerConnector.class);

        // simulate an acknowledged message
        final KafkaMessageId id = new KafkaMessageId(1, 1234);
        final byte[] message = {1, 2, 3, 4};
        _subject._inProgress.put(id, message);
        _subject.ack(id);
        verify(unreliable).ack(id);

        // failure is tested above
    }
}
