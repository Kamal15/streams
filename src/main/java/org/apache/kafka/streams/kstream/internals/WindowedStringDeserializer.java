package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.StringDeserializer;

public class WindowedStringDeserializer extends WindowedDeserializer<String> {

    public WindowedStringDeserializer() {
        super(new StringDeserializer());
    }
}
