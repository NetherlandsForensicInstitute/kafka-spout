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

import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.CONFIG_TOPIC;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.DEFAULT_TOPIC;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.getMaxBufSize;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.getTopic;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.CONFIG_BUFFER_MAX_MESSAGES;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.CONFIG_FILE;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.DEFAULT_BUFFER_MAX_MESSAGES;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.checkConfigSanity;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.configFromPrefix;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.configFromResource;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.createFailHandlerFromString;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.createKafkaConfig;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.getStormZookeepers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import backtype.storm.Config;
import nl.minvenj.nfi.storm.kafka.fail.AbstractFailHandler;
import nl.minvenj.nfi.storm.kafka.fail.FailHandler;
import nl.minvenj.nfi.storm.kafka.fail.ReliableFailHandler;
import nl.minvenj.nfi.storm.kafka.fail.UnreliableFailHandler;

public class ConfigUtilsTest {
    @Test
    public void testPolicyIdentifierSanity() {
        // check sanity on policy identifiers
        assertNotEquals(ReliableFailHandler.IDENTIFIER, UnreliableFailHandler.IDENTIFIER);
        assertNotEquals(new ReliableFailHandler().getIdentifier(), new UnreliableFailHandler().getIdentifier());
    }

    @Test
    public void testCreateKafkaConfigFromResource() {
        final Map<String, Object> stormConfig = new HashMap<String, Object>() {{
            put(CONFIG_FILE, "kafka-config.properties");
        }};

        final Properties config = createKafkaConfig(stormConfig);

        // assert the values in the kafka-config file are present and have been read correctly
        assertEquals("non-existent.host:2181", config.getProperty("zookeeper.connect"));
        assertEquals("100", config.getProperty("consumer.timeout.ms"));
    }

    @Test
    public void testCreateKafkaConfigFromStorm() {
        final Map<String, Object> stormConfig = new HashMap<String, Object>() {{
            put("kafka.zookeeper.connect", "non-existent.host:2181");
            put("kafka.consumer.timeout.ms", "100");
            put("kafka.property.that.makes.little.sense", "nonsense");
        }};

        final Properties config = createKafkaConfig(stormConfig);

        // assert existence of values for keys without the prefix
        assertEquals("non-existent.host:2181", config.getProperty("zookeeper.connect"));
        assertEquals("nonsense", config.getProperty("property.that.makes.little.sense"));
    }

    @Test
    public void testCreateKafkaConfigZookeeperOverride() {
        final Map<String, Object> stormConfig = new HashMap<String, Object>() {{
            put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("non-existent.host"));
            put(Config.STORM_ZOOKEEPER_PORT, 2181);
            put("kafka.consumer.timeout.ms", "100");
        }};

        final Properties config = createKafkaConfig(stormConfig);

        // assert that the value used for kafka (zookeeper.connect) is derived from the value configured for storm
        assertEquals("non-existent.host:2181", config.getProperty("zookeeper.connect"));
    }

    @Test
    public void testCreateKafkaConfigMissingZookeeper() {
        final Map<String, Object> stormConfig = new HashMap<String, Object>() {{
            put("kafka.consumer.timeout.ms", "100");
        }};

        try {
            final Properties config = createKafkaConfig(stormConfig);
            fail("missing zookeeper configuration not detected");
        }
        catch (final IllegalArgumentException e) {
            // this is expected, zookeeper configuration is missing
        }
    }

    @Test
    public void testSanityCheckSuccess() {
        final Properties properties = new Properties();
        properties.setProperty("consumer.timeout.ms", "35");

        // check sanity (should not raise exception
        checkConfigSanity(properties);
    }

    @Test
    public void testSanityCheckFailure() {
        final Properties properties = new Properties();
        // set blocking operation of consumer
        properties.setProperty("consumer.timeout.ms", "-1");

        try {
            checkConfigSanity(properties);
        }
        catch (final IllegalArgumentException e) {
            // this is expected, blocking consumer config should be rejected
        }
    }

    @Test
    public void testConfigFromResource() {
        // load from file in resources
        final Properties fromFile = configFromResource("test-config.properties");

        assertEquals(6, fromFile.size());
        assertEquals("value", fromFile.getProperty("key"));
        assertEquals("silly value", fromFile.getProperty("dashed-key"));
        assertEquals("sillier value", fromFile.getProperty("a.test.property"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigFromResourceMissing() {
        // file should not exist
        final Properties fromFile = configFromResource("non-existent-file.properties");
        fail("loaded non-existent-file.properties from classpath");
    }

    @Test
    public void testConfigFromPrefix() {
        final Properties fromFile = configFromResource("test-config.properties");
        final Properties prefixed = configFromPrefix((Map) fromFile, "prefix.");

        // test file contains two prefixed keys, should be available without prefix
        assertEquals(2, prefixed.size());
        assertEquals("value", prefixed.getProperty("key"));
        assertEquals("another value", prefixed.getProperty("another.key"));
    }

    @Test
    public void testGetStormZookeepers() {
        final Map<String, Object> stormConfig = new HashMap<String, Object>() {{
            put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("non-existent.host", "zookeeper.example.net"));
            put(Config.STORM_ZOOKEEPER_PORT, 1234);
        }};

        // result should be "<server1>:<port>,<server2>:<port>"
        final String zookeepers = getStormZookeepers(stormConfig);
        assertThat(zookeepers, containsString("non-existent"));
        assertThat(zookeepers, containsString("example.net"));
        assertThat(zookeepers, containsString(":1234"));
    }

    @Test
    public void testGetStormZookeepersFail() {
        // port is not a number
        String zookeepers = getStormZookeepers(new HashMap<String, Object>() {{
            put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("non-existent.host", "zookeeper.example.net"));
            put(Config.STORM_ZOOKEEPER_PORT, "not a number");
        }});
        assertNull(zookeepers);

        // servers is missing
        zookeepers = getStormZookeepers(Collections.singletonMap(Config.STORM_ZOOKEEPER_PORT, (Object) "1234"));
        assertNull(zookeepers);

        // no configuration keys
        zookeepers = getStormZookeepers(new HashMap<String, Object>());
        assertNull(zookeepers);
    }

    @Test
    public void testCreateFailHandlerFromString() {
        assertTrue(createFailHandlerFromString("reliable") instanceof ReliableFailHandler);
        assertTrue(createFailHandlerFromString("unreliable") instanceof UnreliableFailHandler);
        // load from class name known to be a FailHandler implementation
        assertTrue(createFailHandlerFromString(TestFailHandler.class.getName()) instanceof FailHandler);
    }

    @Test
    public void testCreateFailHandlerFromStringFail() {
        try {
            // class cannot be loaded, should yield nested ClassNotFoundException
            createFailHandlerFromString("net.example.AbstractClassThatDoesNotActuallyExistImplFactory");
        }
        catch (final IllegalArgumentException e) {
            assertTrue(e.getCause() instanceof ReflectiveOperationException);
        }

        try {
            // class cannot be cast to FailHandler, should yield nested ClassCastException
            createFailHandlerFromString(ConfigUtilsTest.class.getName());
        }
        catch (final IllegalArgumentException e) {
            assertTrue(e.getCause() instanceof ClassCastException);
        }
    }

    @Test
    public void testGetMaxBufSize() {
        // use a value not equal to the default
        Map<String, Object> stormConfig = Collections.singletonMap(CONFIG_BUFFER_MAX_MESSAGES, (Object) (DEFAULT_BUFFER_MAX_MESSAGES * 2));
        assertEquals(DEFAULT_BUFFER_MAX_MESSAGES * 2, getMaxBufSize(stormConfig));

        // assert use of default on missing value
        assertEquals(DEFAULT_BUFFER_MAX_MESSAGES, getMaxBufSize(new HashMap<String, Object>()));

        // assert use of default on invalid value
        stormConfig = Collections.singletonMap(CONFIG_BUFFER_MAX_MESSAGES, (Object) "not a number");
        assertEquals(DEFAULT_BUFFER_MAX_MESSAGES, getMaxBufSize(stormConfig));
    }

    @Test
    public void testGetTopic() {
        // assert use of default on missing value
        assertEquals(DEFAULT_TOPIC, getTopic(new HashMap<String, Object>()));

        Map<String, Object> stormConfig = Collections.singletonMap(CONFIG_TOPIC, (Object) "");
        // assert use of default on empty value
        assertEquals(DEFAULT_TOPIC, getTopic(stormConfig));

        // assert use of default on trimmed empty value
        stormConfig = Collections.singletonMap(CONFIG_TOPIC, (Object) "  ");
        assertEquals(DEFAULT_TOPIC, getTopic(stormConfig));

        // assert use of configured value
        stormConfig = Collections.singletonMap(CONFIG_TOPIC, (Object) "test-topic");
        assertEquals("test-topic", getTopic(stormConfig));

        // assert configured value is trimmed
        stormConfig = Collections.singletonMap(CONFIG_TOPIC, (Object) "  test-topic  ");
        assertEquals("test-topic", getTopic(stormConfig));
    }

    /**
     * Dummy implementation of FailHandler.
     */
    protected static class TestFailHandler extends AbstractFailHandler {
        @Override
        public boolean shouldReplay(final KafkaMessageId id) {
            return false;
        }

        @Override
        public String getIdentifier() {
            return "test";
        }
    }
}
