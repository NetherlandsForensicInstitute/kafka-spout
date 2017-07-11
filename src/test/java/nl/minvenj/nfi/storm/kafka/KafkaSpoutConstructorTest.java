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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import nl.minvenj.nfi.storm.kafka.util.ConfigUtils;

public class KafkaSpoutConstructorTest {
    /**
     * Using the default constructor, a topic name must be in the storm config
     */
    @Test
    public void testOpenWithDefaultConstructor() {
        KafkaSpout spout = spy(new KafkaSpout());

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);

        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ConfigUtils.CONFIG_TOPIC, "topic");
        doNothing().when(spout).createConsumer(config);

        spout.open(config, topology, collector);

        assertEquals("Wrong Topic Name", spout._topic, "topic");
    }

    /**
     * Using the default constructor, a topic name must be in the storm config.
     */
    @Test
    public void testOpenWithDefaultTopicName() {
        KafkaSpout spout = spy(new KafkaSpout());

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);

        Map<String, Object> config = new HashMap<String, Object>();
        doNothing().when(spout).createConsumer(config);

        spout.open(config, topology, collector);

        assertEquals("Wrong Topic Name", spout._topic, ConfigUtils.DEFAULT_TOPIC);
    }

    /**
     * If we use the overloaded constructor, do not even look at the storm config for the topic name.
     */
    @Test
    public void testOpenWithOverloadedConstructor() {
        KafkaSpout spout = spy(new KafkaSpout("OVERLOAD"));

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);

        Map<String, Object> config = new HashMap<String, Object>();
        doNothing().when(spout).createConsumer(config);

        spout.open(config, topology, collector);
        assertEquals("Wrong Topic Name", spout._topic, "OVERLOAD");
    }

    /**
     * If we use the overloaded constructor, with the topic name, it does not matter what is in the storm config.
     */
    @Test
    public void testOpenWithOverloadedConstructorAndStormConfig() {
        KafkaSpout spout = spy(new KafkaSpout("OVERLOAD"));

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);

        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ConfigUtils.CONFIG_TOPIC, "topic");
        doNothing().when(spout).createConsumer(config);

        spout.open(config, topology, collector);

        assertEquals("Wrong Topic Name", spout._topic, "OVERLOAD");
    }

    @Test
    public void testRawSchemeForDefaultConstructor() {
        final KafkaSpout spout = spy(new KafkaSpout());
        final OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

        spout.declareOutputFields(declarer);

        // Fields doesn't implement equals; match it manually
        verify(declarer).declare(argThat(new ArgumentMatcher<Fields>() {
            @Override
            public boolean matches(final Object argument) {
                final Fields fields = (Fields) argument;
                return fields.size() == 1 && fields.get(0).equals("bytes");
            }
        }));
    }

    @Test
    public void testDelegateCustomScheme() {
        final Scheme scheme = new Scheme() {
            @Override
            public List<Object> deserialize(final ByteBuffer bytes) {
                final byte[] result = new byte[bytes.limit() - 1];
                bytes.get(result, 1, bytes.limit());

                return Arrays.<Object>asList(
                    new byte[]{bytes.get()},
                    result
                );
            }

            @Override
            public Fields getOutputFields() {
                return new Fields("head", "tail");
            }
        };
        final OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

        // test for both constructors that accept a scheme
        new KafkaSpout(scheme).declareOutputFields(declarer);
        new KafkaSpout("topic", scheme).declareOutputFields(declarer);

        // Fields doesn't implement equals; match it manually
        verify(declarer, times(2)).declare(argThat(new ArgumentMatcher<Fields>() {
            @Override
            public boolean matches(final Object argument) {
                final Fields fields = (Fields) argument;
                return fields.size() == 2 && fields.get(0).equals("head") && fields.get(1).equals("tail");
            }
        }));
    }
}
