package nl.minvenj.nfi.storm.kafka;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
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
}
