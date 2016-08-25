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

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;

/**
 * Abstract convenience implementation of the {@link FailHandler} interface.
 *
 * @author Netherlands Forensics Institute
 */
public abstract class AbstractFailHandler implements FailHandler {
    @Override
    public abstract boolean shouldReplay(final KafkaMessageId id);

    @Override
    public void ack(final KafkaMessageId id) {
    }

    @Override
    public void fail(final KafkaMessageId id, final byte[] message) {
    }

    @Override
    public void open(final Map config, final TopologyContext topology, final SpoutOutputCollector collector) {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void close() {
    }

    @Override
    public abstract String getIdentifier();
}
