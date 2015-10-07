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

import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.CONFIG_FAIL_HANDLER;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.DEFAULT_FAIL_HANDLER;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.createFailHandlerFromString;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.createKafkaConfig;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.getMaxBufSize;
import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.getTopic;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;

import kafka.message.InvalidMessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import nl.minvenj.nfi.storm.kafka.fail.FailHandler;
import nl.minvenj.nfi.storm.kafka.util.ConfigUtils;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;

/**
 * Storm spout reading messages from kafka, emitting them as single field tuples.
 *
 * Implementation tracks a queue of message ids (partition and offset) and a set of those ids that are pending to be
 * acknowledged by the topology. The buffer will only be populated with new message when *all* messages from the buffer
 * have been acknowledged because the {@link ConsumerConnector} allows committing of the currently processed offset only
 * through {@link kafka.javaapi.consumer.ConsumerConnector#commitOffsets()}, which commits *all* offsets that have been
 * read, which does not necessarily correspond to the offsets that were successfully processed by the storm topology.
 * Optimizing this behaviour is work for the (near) future.
 *
 * Aside from the properties used to configure the kafka consumer, the kafka spout reads the following configuration
 * parameters in storm configuration:
 * <ul>
 * <li>{@code kafka.spout.topic}: the kafka topic to read messages from (default {@code storm});</li>
 * <li>{@code kafka.spout.fail.handler}: the policy to be used when messages fail, whether to replay them, default
 * {@code "reliable"} (either {@code "reliable"}, {@code "unreliable"} or a fully qualified class name of an
 * implementation of {@link FailHandler});</li>
 * <li>{@code kafka.spout.consumer.group}: The kafka consumer group id.</li>
 * <li>{@code kafka.spout.buffer.size.max}: The maximum number of kafka messages to buffer.</li>
 * </ul>
 *
 * @author Netherlands Forensics Institute
 */
public class KafkaSpout implements IRichSpout {
    private static final long serialVersionUID = -1L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    protected final Scheme _serializationScheme;
    /**
     * Collection of messages being processed by the topology (either waiting to be emitted or waiting to be
     * acknowledged). Processed message offset is committed when this is becomes empty.
     *
     * @see #fillBuffer()
     */
    protected final SortedMap<KafkaMessageId, byte[]> _inProgress = new TreeMap<KafkaMessageId, byte[]>();
    /**
     * Queue of messages waiting to be emitted by this spout.
     *
     * @see #fillBuffer()
     */
    protected final Queue<KafkaMessageId> _queue = new LinkedList<KafkaMessageId>();
    protected String _topic;
    protected int _bufSize;
    protected FailHandler _failHandler;
    protected ConsumerIterator<byte[], byte[]> _iterator;
    protected transient SpoutOutputCollector _collector;
    protected transient ConsumerConnector _consumer;

    /**
     * Creates a new kafka spout to be submitted in a storm topology. Configuration is read from storm config when the
     * spout is opened. Uses a {@link RawScheme} to serialize messages from kafka as a single {@code byte[]}.
     */
    public KafkaSpout() {
        _serializationScheme = new RawScheme();
    }

    /**
     * Creates a new kafka spout to be submitted in a storm topology with the provided {@link Scheme}. This impacts
     * output fields, see {@link #declareOutputFields(OutputFieldsDeclarer)}). Configuration is read from storm config
     * when the spout is opened.
     *
     * @param serializationScheme The serialization to apply to messages read from kafka.
     */
    public KafkaSpout(final Scheme serializationScheme) {
        _serializationScheme = serializationScheme;
    }

    /**
     * Creates a new kafka spout to be submitted in a storm topology. Configuration is read from storm config when the
     * spout is opened.
     *
     * @param topic The kafka topic to read messages from.
     */
    public KafkaSpout(final String topic) {
        this();
        _topic = topic;
    }

    /**
     * Creates a new kafka spout to be submitted in a storm topology with the provided {@link Scheme}. This impacts
     * output fields, see {@link #declareOutputFields(OutputFieldsDeclarer)}). Configuration is read from storm config
     * when the spout is opened.
     *
     * @param topic               The kafka topic to read messages from.
     * @param serializationScheme The serialization to apply to messages read from kafka.
     */
    public KafkaSpout(final String topic, final Scheme serializationScheme) {
        this(serializationScheme);
        _topic = topic;
    }

    /**
     * Convenience method assigning a {@link FailHandler} instance to this kafka spout. If the configured value is
     * {@code null}, {@link ConfigUtils#DEFAULT_FAIL_HANDLER} will be used, otherwise the creation is delegated to
     * {@link ConfigUtils#createFailHandlerFromString(String)}.
     *
     * @param failHandler The configuration value for the failure policy.
     */
    protected void createFailHandler(final String failHandler) {
        if (failHandler == null) {
            _failHandler = DEFAULT_FAIL_HANDLER;
        }
        else {
            _failHandler = createFailHandlerFromString(failHandler);
        }
    }

    /**
     * Ensures an initialized kafka {@link ConsumerConnector} is present.
     *
     * @param config The storm configuration passed to {@link #open(Map, TopologyContext, SpoutOutputCollector)}.
     * @throws IllegalArgumentException When a required configuration parameter is missing or a sanity check fails.
     */
    protected void createConsumer(final Map<String, Object> config) {
        final Properties consumerConfig = createKafkaConfig(config);

        LOG.info("connecting kafka client to zookeeper at {} as client group {}",
            consumerConfig.getProperty("zookeeper.connect"),
            consumerConfig.getProperty("group.id"));
        _consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerConfig));
    }

    /**
     * Refills the buffer with messages from the configured kafka topic if available.
     *
     * @return Whether the buffer contains messages to be emitted after this call.
     * @throws IllegalStateException When current buffer is not empty or messages not acknowledged by topology.
     */
    protected boolean fillBuffer() {
        if (!_inProgress.isEmpty() || !_queue.isEmpty()) {
            throw new IllegalStateException("cannot fill buffer when buffer or pending messages are non-empty");
        }

        if (_iterator == null) {
            // create a stream of messages from _consumer using the streams as defined on construction
            final Map<String, List<KafkaStream<byte[], byte[]>>> streams = _consumer.createMessageStreams(Collections.singletonMap(_topic, 1));
            _iterator = streams.get(_topic).get(0).iterator();
        }

        // We'll iterate the stream in a try-clause; kafka stream will poll its client channel for the next message,
        // throwing a ConsumerTimeoutException when the configured timeout is exceeded.
        try {
            int size = 0;
            while (size < _bufSize && _iterator.hasNext()) {
                final MessageAndMetadata<byte[], byte[]> message = _iterator.next();
                final KafkaMessageId id = new KafkaMessageId(message.partition(), message.offset());
                _inProgress.put(id, message.message());
                size++;
            }
        }
        catch (final InvalidMessageException e) {
            LOG.warn(e.getMessage(), e);
        }
        catch (final ConsumerTimeoutException e) {
            // ignore, storm will call nextTuple again at some point in the near future
            // timeout does *not* mean that no messages were read (state is checked below)
        }

        if (_inProgress.size() > 0) {
            // set _queue to all currently pending kafka message ids
            _queue.addAll(_inProgress.keySet());
            LOG.debug("buffer now has {} messages to be emitted", _queue.size());
            // message(s) appended to buffer
            return true;
        }
        else {
            // no messages appended to buffer
            return false;
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        // delegate fields mapping to specified scheme (single field "bytes" by default)
        declarer.declare(_serializationScheme.getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(final Map config, final TopologyContext topology, final SpoutOutputCollector collector) {
        _collector = collector;

        if (_topic == null) {
            _topic = getTopic((Map<String, Object>) config);
        }

        _bufSize = getMaxBufSize((Map<String, Object>) config);

        createFailHandler((String) config.get(CONFIG_FAIL_HANDLER));

        // ensure availability of kafka consumer
        createConsumer((Map<String, Object>) config);

        // inform the failure policy of spout being opened
        _failHandler.open(config, topology, collector);

        LOG.info("kafka spout opened, reading from topic {}, using failure policy {}", _topic, _failHandler.getIdentifier());
    }

    @Override
    public void close() {
        // reset state by setting members to null
        _collector = null;
        _iterator = null;

        if (_consumer != null) {
            try {
                _consumer.shutdown();
            }
            finally {
                _consumer = null;
            }
        }

        _failHandler.close();
    }

    @Override
    public void activate() {
        _failHandler.activate();
    }

    @Override
    public void deactivate() {
        _failHandler.deactivate();
    }

    @Override
    public void nextTuple() {
        // next tuple available when _queue contains ids or fillBuffer() is allowed and indicates more messages were available
        // see class documentation for implementation note on the rationale behind this condition
        if (!_queue.isEmpty() || (_inProgress.isEmpty() && fillBuffer())) {
            final KafkaMessageId nextId = _queue.poll();
            if (nextId != null) {
                final byte[] message = _inProgress.get(nextId);
                // the next id from buffer should correspond to a message in the pending map
                if (message == null) {
                    throw new IllegalStateException("no pending message for next id " + nextId);
                }
                // use specified scheme to deserialize messages (single-field Values by default)
                _collector.emit(_serializationScheme.deserialize(message), nextId);
                LOG.debug("emitted kafka message id {} ({} bytes payload)", nextId, message.length);
            }
        }
    }

    @Override
    public void ack(final Object o) {
        if (o instanceof KafkaMessageId) {
            final KafkaMessageId id = (KafkaMessageId) o;
            // message corresponding to o is no longer pending
            _inProgress.remove(id);
            LOG.debug("kafka message {} acknowledged", id);
            if (_inProgress.isEmpty()) {
                // commit offsets to zookeeper when pending is now empty
                // (buffer will be filled on next call to nextTuple())
                LOG.debug("all pending messages acknowledged, committing client offsets");
                _consumer.commitOffsets();
            }
            // notify fail handler of tuple success
            _failHandler.ack(id);
        }
    }

    @Override
    public void fail(final Object o) {
        if (o instanceof KafkaMessageId) {
            final KafkaMessageId id = (KafkaMessageId) o;
            // delegate decision of replaying the message to failure policy
            if (_failHandler.shouldReplay(id)) {
                LOG.debug("kafka message id {} failed in topology, adding to buffer again", id);
                _queue.add(id);
            }
            else {
                LOG.debug("kafka message id {} failed in topology, delegating failure to policy", id);
                // remove message from pending; _failHandler will take action if needed
                _failHandler.fail(id, _inProgress.remove(id));
            }
        }
    }
}
