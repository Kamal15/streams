package io.confluent.examples.streams.utils;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class SpecificAvroSerde<T extends SpecificRecord> implements Serde<T> {

    private final Serde<T> inner;

    public SpecificAvroSerde() {
        inner = Serdes.serdeFrom(new SpecificAvroSerializer<>(), new SpecificAvroDeserializer<>());
    }

    public SpecificAvroSerde(SchemaRegistryClient client) {
        inner = Serdes.serdeFrom(new SpecificAvroSerializer<>(client), new SpecificAvroDeserializer<>(client));
    }

    public SpecificAvroSerde(SchemaRegistryClient client, Map<String, ?> props) {
        inner = Serdes.serdeFrom(new SpecificAvroSerializer<>(client, props), new SpecificAvroDeserializer<>(client, props));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }

    @Override
    public Serializer<T> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return inner.deserializer();
    }
}
