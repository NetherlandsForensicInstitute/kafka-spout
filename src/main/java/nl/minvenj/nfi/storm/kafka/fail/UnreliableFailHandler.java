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
 * {@link FailHandler} implementation making tuple failure unreliable: messages are never replayed and calls to
 * {@link #fail(nl.minvenj.nfi.storm.kafka.util.KafkaMessageId, byte[])} are ignored.
 *
 * @author Netherlands Forensics Institute
 */
public class UnreliableFailHandler extends AbstractFailHandler {
    /**
     * Configuration identifier for the unreliable failure policy ({@code "unreliable"}).
     */
    public static final String IDENTIFIER = "unreliable";

    @Override
    public boolean shouldReplay(final KafkaMessageId id) {
        // never replay a message, ignore calls to fail; lose the message in time and space
        return false;
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }
}
