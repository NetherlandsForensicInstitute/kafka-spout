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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import nl.minvenj.nfi.storm.kafka.fail.FailHandler;
import nl.minvenj.nfi.storm.kafka.fail.ReliableFailHandler;
import nl.minvenj.nfi.storm.kafka.fail.UnreliableFailHandler;

/**
 * Utilities for {@link nl.minvenj.nfi.storm.kafka.KafkaSpout} regarding its configuration and reading values from the
 * storm configuration.
 *
 * @author Netherlands Forensics Institute
 */
public class ConfigUtils {
    /**
     * Configuration key prefix in storm config for kafka configuration parameters ({@code "kafka."}). The prefix is
     * stripped from all keys that use it and passed to kafka (see class documentation for additional required values).
     *
     * @see <a href="http://kafka.apache.org/documentation.html#consumerconfigs">kafka documentation</a>
     */
    public static final String CONFIG_PREFIX = "kafka.";
    /**
     * Storm configuration key pointing to a file containing kafka configuration ({@code "kafka.config"}).
     */
    public static final String CONFIG_FILE = "kafka.config";
    /**
     * Storm configuration key used to determine the kafka topic to read from ({@code "kafka.spout.topic"}).
     */
    public static final String CONFIG_TOPIC = "kafka.spout.topic";
    /**
     * Default kafka topic to read from ({@code "storm"}).
     */
    public static final String DEFAULT_TOPIC = "storm";
    /**
     * Storm configuration key used to determine the failure policy to use ({@code "kafka.spout.fail.handler"}).
     */
    public static final String CONFIG_FAIL_HANDLER = "kafka.spout.fail.handler";
    /**
     * Default failure policy instance (a {@link ReliableFailHandler} instance).
     */
    public static final FailHandler DEFAULT_FAIL_HANDLER = new ReliableFailHandler();
    /**
     * Storm configuration key used to determine the failure policy to use ({@code "kafka.spout.consumer.group"}).
     */
    public static final String CONFIG_GROUP = "kafka.spout.consumer.group";
    /**
     * Default kafka consumer group id ({@code "kafka_spout"}).
     */
    public static final String DEFAULT_GROUP = "kafka_spout";
    /**
     * Storm configuration key used to determine the maximum number of message to buffer
     * ({@code "kafka.spout.buffer.size.max"}).
     */
    public static final String CONFIG_BUFFER_MAX_MESSAGES = "kafka.spout.buffer.size.max";
    /**
     * Default maximum buffer size in number of messages ({@code 1024}).
     */
    public static final int DEFAULT_BUFFER_MAX_MESSAGES = 1024;
    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * Reads configuration from a classpath resource stream obtained from the current thread's class loader through
     * {@link ClassLoader#getSystemResourceAsStream(String)}.
     *
     * @param resource The resource to be read.
     * @return A {@link java.util.Properties} object read from the specified resource.
     * @throws IllegalArgumentException When the configuration file could not be found or another I/O error occurs.
     */
    public static Properties configFromResource(final String resource) {
        InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        if (input == null) {
            // non-existent resource will *not* throw an exception, do this anyway
            throw new IllegalArgumentException("configuration file '" + resource + "' not found on classpath");
        }

        final Properties config = new Properties();
        try {
            config.load(input);
        }
        catch (final IOException e) {
            throw new IllegalArgumentException("reading configuration from '" + resource + "' failed", e);
        }
        return config;
    }

    /**
     * Creates a {@link Properties} object to create the consumer configuration for the kafka spout.
     *
     * @param config The storm configuration mapping.
     * @return Configuration for a kafka consumer encoded as a {@link Properties} object.
     * @throws IllegalArgumentException When required configuration parameters are missing or sanity checks fail.
     */
    public static Properties createKafkaConfig(final Map<String, Object> config) {
        final Properties consumerConfig;
        if (config.get(CONFIG_FILE) != null) {
            final String configFile = String.valueOf(config.get(CONFIG_FILE));
            // read values from separate config file
            LOG.info("loading kafka configuration from {}", configFile);
            consumerConfig = configFromResource(configFile);
        }
        else {
            // configuration file not set, read values from storm config with kafka prefix
            LOG.info("reading kafka configuration from storm config using prefix '{}'", CONFIG_PREFIX);
            consumerConfig = configFromPrefix(config, CONFIG_PREFIX);
        }

        // zookeeper connection string is critical, try to make sure it's present
        if (!consumerConfig.containsKey("zookeeper.connect")) {
            final String zookeepers = getStormZookeepers(config);
            if (zookeepers != null) {
                consumerConfig.setProperty("zookeeper.connect", zookeepers);
                LOG.info("no explicit zookeeper configured for kafka, falling back on storm's zookeeper ({})", zookeepers);
            }
            else {
                // consumer will fail to start without zookeeper.connect
                throw new IllegalArgumentException("required kafka configuration key 'zookeeper.connect' not found");
            }
        }

        // group id string is critical, try to make sure it's present
        if (!consumerConfig.containsKey("group.id")) {
            final Object groupId = config.get(CONFIG_GROUP);
            if (groupId != null) {
                consumerConfig.setProperty("group.id", String.valueOf(groupId));
            }
            else {
                consumerConfig.setProperty("group.id", DEFAULT_GROUP);
                LOG.info("kafka consumer group id not configured, using default ({})", DEFAULT_GROUP);
            }
        }

        // check configuration sanity before returning
        checkConfigSanity(consumerConfig);
        return consumerConfig;
    }

    /**
     * Reads a configuration subset from storm's configuration, stripping {@code prefix} from keys using it.
     *
     * @param base   Storm's configuration mapping.
     * @param prefix The prefix to match and strip from the beginning.
     * @return A {@link Properties} object created from storm's configuration.
     */
    public static Properties configFromPrefix(final Map<String, Object> base, final String prefix) {
        final Properties config = new Properties();
        // load configuration from base, stripping prefix
        for (Map.Entry<String, Object> entry : base.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                config.setProperty(entry.getKey().substring(prefix.length()), String.valueOf(entry.getValue()));
            }
        }

        return config;
    }

    /**
     * Creates a zookeeper connect string usable for the kafka configuration property {@code "zookeeper.connect"} from
     * storm's configuration map by looking up the {@link backtype.storm.Config#STORM_ZOOKEEPER_SERVERS} and
     * {@link backtype.storm.Config#STORM_ZOOKEEPER_PORT} values. Returns null when this procedure fails.
     *
     * @param stormConfig Storm's configuration map.
     * @return A zookeeper connect string if it can be created from storm's config or null.
     */
    public static String getStormZookeepers(final Map<String, Object> stormConfig) {
        final Object stormZookeepers = stormConfig.get(Config.STORM_ZOOKEEPER_SERVERS);
        final Object stormZookeepersPort = stormConfig.get(Config.STORM_ZOOKEEPER_PORT);
        if (stormZookeepers instanceof List && stormZookeepersPort instanceof Number) {
            // join the servers and the port together to a single zookeeper connection string for kafka
            final StringBuilder zookeepers = new StringBuilder();
            final int port = ((Number) stormZookeepersPort).intValue();

            for (final Iterator<?> iterator = ((List) stormZookeepers).iterator(); iterator.hasNext(); ) {
                zookeepers.append(String.valueOf(iterator.next()));
                zookeepers.append(':');
                zookeepers.append(port);
                if (iterator.hasNext()) {
                    zookeepers.append(',');
                }
            }
            return zookeepers.toString();
        }

        // no valid zookeeper configuration found
        return null;
    }

    /**
     * Creates a {@link FailHandler} implementation from a string argument. If the argument fails to qualify as either
     * {@link ReliableFailHandler#IDENTIFIER} or {@link UnreliableFailHandler#IDENTIFIER}, the argument is interpreted
     * as a class name and instantiated through {@code Class.forName(failHandler).newInstance()}.
     *
     * @param failHandler A fail handler identifier or class name.
     * @return A {@link FailHandler} instance.
     * @throws IllegalArgumentException When instantiating {@code failHandler} fails or is not a {@link FailHandler}.
     */
    public static FailHandler createFailHandlerFromString(final String failHandler) {
        // determine fail handler implementation from string value
        if (failHandler.equalsIgnoreCase(ReliableFailHandler.IDENTIFIER)) {
            return new ReliableFailHandler();
        }
        else if (failHandler.equalsIgnoreCase(UnreliableFailHandler.IDENTIFIER)) {
            return new UnreliableFailHandler();
        }
        else {
            // create fail handler using parameter as identifier or class name
            try {
                return (FailHandler) Class.forName(failHandler).newInstance();
            }
            catch (final ClassNotFoundException e) {
              throw new IllegalArgumentException("failed to instantiate FailHandler instance from argument " +
                  failHandler, e);
            }
            catch (final InstantiationException e) {
              throw new IllegalArgumentException("failed to instantiate FailHandler instance from argument " +
                  failHandler, e);
            }
            catch (final IllegalAccessException e) {
              throw new IllegalArgumentException("failed to instantiate FailHandler instance from argument " +
                  failHandler, e);
            }
            catch (final ClassCastException e) {
                throw new IllegalArgumentException("instance from argument " + failHandler +
                    " does not implement FailHandler", e);
            }
        }
    }

    /**
     * Retrieves the maximum buffer size to be used from storm's configuration map, or the
     * {@link #DEFAULT_BUFFER_MAX_MESSAGES} if no such value was found using {@link #CONFIG_BUFFER_MAX_MESSAGES}.
     *
     * @param stormConfig Storm's configuration map.
     * @return The maximum buffer size to use.
     */
    public static int getMaxBufSize(final Map<String, Object> stormConfig) {
        final Object value = stormConfig.get(CONFIG_BUFFER_MAX_MESSAGES);
        if (value != null) {
            try {
                return Integer.parseInt(String.valueOf(value).trim());
            }
            catch (final NumberFormatException e) {
                LOG.warn("invalid value for '{}' in storm config ({}); falling back to default ({})", CONFIG_BUFFER_MAX_MESSAGES, value, DEFAULT_BUFFER_MAX_MESSAGES);
            }
        }

        return DEFAULT_BUFFER_MAX_MESSAGES;
    }

    /**
     * Retrieves the topic to be consumed from storm's configuration map, or the {@link #DEFAULT_TOPIC} if no
     * (non-empty) value was found using {@link #CONFIG_TOPIC}.
     *
     * @param stormConfig Storm's configuration map.
     * @return The topic to be consumed.
     */
    public static String getTopic(final Map<String, Object> stormConfig) {
        if (stormConfig.containsKey(CONFIG_TOPIC)) {
            // get configured topic from config as string, removing whitespace from both ends
            final String topic = String.valueOf(stormConfig.get(CONFIG_TOPIC)).trim();
            if (topic.length() > 0) {
                return topic;
            }
            else {
                LOG.warn("configured topic found in storm config is empty, defaulting to topic '{}'", DEFAULT_TOPIC);
                return DEFAULT_TOPIC;
            }
        }
        else {
            LOG.warn("no configured topic found in storm config, defaulting to topic '{}'", DEFAULT_TOPIC);
            return DEFAULT_TOPIC;
        }
    }

    /**
     * Checks the sanity of a kafka consumer configuration for use in storm.
     *
     * @param config The configuration parameters to check.
     * @throws IllegalArgumentException When a sanity check fails.
     */
    public static void checkConfigSanity(final Properties config) {
        // consumer timeout should not block calls indefinitely
        final Object consumerTimeout = config.getProperty("consumer.timeout.ms");
        if (consumerTimeout == null || Integer.parseInt(String.valueOf(consumerTimeout)) < 0) {
            throw new IllegalArgumentException("kafka configuration value for 'consumer.timeout.ms' is not suitable for operation in storm");
        }
    }
}
