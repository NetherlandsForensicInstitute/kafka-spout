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

import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;

/**
 * {@link FailHandler} implementation making tuple failure reliable: messages are always replayed, calls to
 * {@link #fail(nl.minvenj.nfi.storm.kafka.util.KafkaMessageId, byte[])} will cause an error.
 *
 * @author Netherlands Forensics Institute
 */
public class ReliableFailHandler extends AbstractFailHandler {
    /**
     * Configuration identifier for the reliable failure policy ({@code "reliable"}).
     */
    public static final String IDENTIFIER = "reliable";

    @Override
    public boolean shouldReplay(final KafkaMessageId id) {
        // always replay the message, never call fail
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException always; tuples should always replayed using reliable policy.
     */
    @Override
    public void fail(final KafkaMessageId id, final byte[] message) {
        throw new IllegalStateException("reliable failure policy unexpectedly made to deal with message failure");
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }
}
